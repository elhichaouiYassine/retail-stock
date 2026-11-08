package com.retail.stock.infrastructure.outbox

import com.retail.stock.infrastructure.config.OutboxConfig
import com.retail.stock.infrastructure.kafka.dispatch
import com.retail.stock.infrastructure.resilience.Backoff
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Drains the PostgreSQL outbox into Kafka.
 *
 * ```
 *  [poller] --claim(SKIP LOCKED)--> Channel(bufferedBatches) --> [publisher] --send--> Kafka (1 broker)
 *     ^                                   (suspends when full)        |
 *     +------ backoff while broker is down <---- consecutive failures +--> ack: DELETE / fail: reschedule / DLQ
 * ```
 *
 * - The bounded [Channel] is the backpressure boundary: if Kafka slows down, the poller suspends instead of
 *   leasing more rows than the publisher can handle.
 * - A single publisher per pod sends records sequentially, so the idempotent producer preserves per-key order
 *   within a pod. Consumers must still use `stockVersion` (monotonic per stock row) because leases can move
 *   rows between pods after a crash.
 * - If the single broker is down, nothing is lost: events accumulate in PostgreSQL and the relay backs off
 *   exponentially, then catches up at batch speed when the broker returns.
 */
class OutboxRelay(
    private val repository: OutboxRepository,
    private val producer: Producer<String, String>,
    private val signal: OutboxSignal,
    private val config: OutboxConfig,
    private val owner: String,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(OutboxRelay::class.java)
    private val consecutiveFailures = AtomicInteger(0)

    private val published: Counter = Counter.builder("retail.stock.outbox.published").register(meterRegistry)
    private val failed: Counter = Counter.builder("retail.stock.outbox.failed").register(meterRegistry)
    private val deadLettered: Counter = Counter.builder("retail.stock.outbox.dead_lettered").register(meterRegistry)
    private val batchTimer: Timer = Timer.builder("retail.stock.outbox.batch.duration").register(meterRegistry)
    private val brokerDown = AtomicLong(0)

    init {
        meterRegistry.gauge("retail.stock.outbox.broker_unavailable", brokerDown)
    }

    val healthy: Boolean get() = consecutiveFailures.get() == 0

    fun start(scope: CoroutineScope): Job = scope.launch(CoroutineName("outbox-relay")) {
        val batches = Channel<List<OutboxRecord>>(config.bufferedBatches)
        launch(CoroutineName("outbox-publisher")) {
            for (batch in batches) publish(batch)
        }
        try {
            poll(batches)
        } finally {
            batches.close()
        }
    }

    private suspend fun CoroutineScope.poll(batches: Channel<List<OutboxRecord>>) {
        var dbFailures = 0
        while (isActive) {
            val brokerFailures = consecutiveFailures.get()
            if (brokerFailures > 0) {
                delay(Backoff.delayFor(brokerFailures, config.baseBackoff, config.maxBackoff).toMillis())
            }
            val batch = try {
                repository.claim(config.batchSize, config.lease, owner).also { dbFailures = 0 }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                dbFailures++
                log.warn("Outbox claim failed (attempt {}): {}", dbFailures, e.toString())
                delay(Backoff.delayFor(dbFailures, config.baseBackoff, config.maxBackoff).toMillis())
                continue
            }
            if (batch.isEmpty()) {
                signal.awaitPending(config.pollInterval)
                continue
            }
            batches.send(batch)
            if (batch.size < config.batchSize) signal.awaitPending(config.pollInterval)
        }
    }

    private suspend fun publish(batch: List<OutboxRecord>) {
        val started = System.nanoTime()
        val pending = withContext(Dispatchers.IO) {
            batch.map { record -> record to producer.dispatch(record.toProducerRecord()) }
        }

        val succeeded = ArrayList<Long>(batch.size)
        val retry = ArrayList<OutboxFailure>()
        val poison = ArrayList<OutboxFailure>()
        for ((record, ack) in pending) {
            val error: Throwable? = try {
                if (withTimeoutOrNull(config.sendTimeout.toMillis()) { ack.await() } == null) {
                    TimeoutException("No broker acknowledgement within ${config.sendTimeout}")
                } else {
                    null
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                e
            }
            when {
                error == null -> succeeded += record.id
                error.isPoison() || record.attempts + 1 >= config.maxAttempts ->
                    poison += OutboxFailure(record, error.toString(), Duration.ZERO)

                else -> retry += OutboxFailure(
                    record,
                    error.toString(),
                    Backoff.delayFor(record.attempts + 1, config.baseBackoff, config.maxBackoff),
                )
            }
        }

        try {
            repository.markPublished(succeeded, owner)
            repository.reschedule(retry, owner)
            repository.deadLetter(poison, owner)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Kafka already has the records; the lease will expire and they will be re-sent (at-least-once).
            log.error(
                "Failed to record outbox publish outcome for {} events; they will be re-sent after lease expiry",
                batch.size,
                e
            )
        }

        published.increment(succeeded.size.toDouble())
        failed.increment(retry.size.toDouble())
        deadLettered.increment(poison.size.toDouble())
        batchTimer.record(Duration.ofNanos(System.nanoTime() - started))

        if (succeeded.isEmpty() && (retry.isNotEmpty() || poison.isNotEmpty())) {
            val failures = consecutiveFailures.incrementAndGet()
            brokerDown.set(1)
            log.warn(
                "Kafka publish failed for whole batch of {} (consecutive failures: {}): {}",
                batch.size,
                failures,
                retry.firstOrNull()?.error
            )
        } else {
            if (consecutiveFailures.getAndSet(0) > 0) log.info("Kafka publishing recovered")
            brokerDown.set(0)
        }
        if (poison.isNotEmpty()) log.error("{} outbox events moved to dead_letters", poison.size)
    }

    private fun OutboxRecord.toProducerRecord(): ProducerRecord<String, String> =
        ProducerRecord<String, String>(topic, null, createdAt.toEpochMilli(), messageKey, payload).apply {
            headers().add("event-id", eventId.toString().toByteArray(StandardCharsets.UTF_8))
            headers().add("event-type", eventType.toByteArray(StandardCharsets.UTF_8))
            headers().add("content-type", "application/json".toByteArray(StandardCharsets.UTF_8))
            headers().add("schema-version", "1".toByteArray(StandardCharsets.UTF_8))
        }

    private fun Throwable.isPoison(): Boolean =
        this is RecordTooLargeException || this is SerializationException
}
