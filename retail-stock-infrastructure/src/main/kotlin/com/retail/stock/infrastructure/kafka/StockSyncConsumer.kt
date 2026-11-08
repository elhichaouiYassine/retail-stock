package com.retail.stock.infrastructure.kafka

import com.retail.stock.domain.command.SyncStock
import com.retail.stock.domain.error.StaleStockCount
import com.retail.stock.domain.error.StockException
import com.retail.stock.domain.model.*
import com.retail.stock.domain.security.*
import com.retail.stock.domain.service.StockService
import com.retail.stock.infrastructure.config.KafkaConfig
import com.retail.stock.infrastructure.dlq.DeadLetterRepository
import com.retail.stock.infrastructure.dlq.DeadLetterSource
import com.retail.stock.infrastructure.dlq.NewDeadLetter
import com.retail.stock.infrastructure.resilience.Backoff
import com.retail.stock.infrastructure.serialization.EventCodec
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.serialization.SerializationException
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.*
import java.util.concurrent.Executors

/** Decodes and applies one `retail.stock.sync.v1` message. Shared by the consumer and DLQ replay. */
class StockSyncHandler(private val service: StockService) {

    sealed interface Outcome {
        data class Applied(val level: StockLevel) : Outcome
        data class Skipped(val reason: String) : Outcome
    }

    /** Throws [PoisonMessage] for messages that can never succeed, other exceptions are transient. */
    suspend fun handle(payload: String): Outcome {
        val command = try {
            val message = EventCodec.decodeSync(payload)
            SyncStock(
                key = StockKey(Sku(message.sku), LocationId(message.locationId)),
                countedOnHand = message.onHand,
                source = message.source,
                countedAt = message.countedAt?.let(Instant::parse),
                idempotencyKey = IdempotencyKey("sync:${UUID.fromString(message.eventId)}"),
            )
        } catch (e: SerializationException) {
            throw PoisonMessage("Undecodable sync message: ${e.message}", e)
        } catch (e: IllegalArgumentException) {
            throw PoisonMessage("Invalid sync message: ${e.message}", e)
        } catch (e: DateTimeParseException) {
            throw PoisonMessage("Invalid countedAt: ${e.message}", e)
        } catch (e: StockException) {
            throw PoisonMessage("Invalid sync message: ${e.message}", e)
        }

        return withContext(ActorContext(SYNC_ACTOR)) {
            try {
                Outcome.Applied(service.sync(command))
            } catch (e: StaleStockCount) {
                Outcome.Skipped(e.message ?: "stale count")
            } catch (e: StockException) {
                throw PoisonMessage("Rejected by domain: ${e.message}", e)
            }
        }
    }

    class PoisonMessage(message: String, cause: Throwable) : RuntimeException(message, cause)

    companion object {
        val SYNC_ACTOR = Actor(
            subject = "kafka:retail.stock.sync.v1",
            kind = ActorKind.SERVICE,
            displayName = "kafka-sync-consumer",
            clientId = "retail-stock",
            permissions = setOf(Permission.STOCK_READ, Permission.STOCK_SYNC),
            locationScope = LocationScope.All,
        )
    }
}

/**
 * At-least-once consumer of physical stock counts.
 *
 * - `KafkaConsumer` is not thread safe, so it lives on one dedicated thread (single-thread dispatcher);
 *   processing suspends onto the DB dispatcher and comes back, never touching the consumer elsewhere.
 * - Offsets are committed per partition only after records are applied or dead-lettered.
 * - Each record is retried with backoff; poison or exhausted records go to the PostgreSQL DLQ.
 * - If even the DLQ write fails (PostgreSQL down) the partition is rewound and the loop backs off:
 *   nothing is skipped, nothing is lost.
 * - Duplicates are harmless: the event id is the idempotency key of the sync command.
 */
class StockSyncConsumer(
    private val config: KafkaConfig,
    private val handler: StockSyncHandler,
    private val deadLetters: DeadLetterRepository,
    private val instanceId: String,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(StockSyncConsumer::class.java)

    @Volatile
    private var consumer: KafkaConsumer<String, String>? = null

    @Volatile
    private var stopping = false

    private val applied = Counter.builder("retail.stock.sync.applied").register(meterRegistry)
    private val skipped = Counter.builder("retail.stock.sync.skipped").register(meterRegistry)
    private val deadLettered = Counter.builder("retail.stock.sync.dead_lettered").register(meterRegistry)

    fun start(scope: CoroutineScope): Job = scope.launch(CoroutineName("kafka-sync-consumer")) {
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "kafka-sync-consumer").apply { isDaemon = true }
        }
        executor.asCoroutineDispatcher().use { pollThread ->
            withContext(pollThread) { runLoop() }
        }
    }

    /** Breaks a blocking `poll()` from another thread; call before cancelling the job. */
    fun wakeup() {
        stopping = true
        consumer?.wakeup()
    }

    private suspend fun CoroutineScope.runLoop() {
        val kafka = KafkaClients.consumer(config, instanceId)
        consumer = kafka
        var fatalFailures = 0
        try {
            kafka.subscribe(listOf(config.syncTopic))
            log.info("Subscribed to {} as group {}", config.syncTopic, config.consumerGroupId)
            while (isActive && !stopping) {
                val records = try {
                    kafka.poll(Duration.ofMillis(500))
                } catch (e: WakeupException) {
                    if (stopping) break else continue
                }
                if (records.isEmpty) continue

                var fatal = false
                for (partition in records.partitions()) {
                    val partitionRecords = records.records(partition)
                    if (fatal) {
                        kafka.seek(partition, partitionRecords.first().offset())
                        continue
                    }
                    var lastDone = -1L
                    for (record in partitionRecords) {
                        if (!process(record)) {
                            fatal = true
                            kafka.seek(partition, record.offset())
                            break
                        }
                        lastDone = record.offset()
                    }
                    if (lastDone >= 0) commit(kafka, partition, lastDone + 1)
                }

                if (fatal) {
                    fatalFailures++
                    val pause = Backoff.delayFor(fatalFailures, Duration.ofSeconds(1), Duration.ofSeconds(30))
                    log.warn("Sync consumer paused {} ms after storage failure (#{})", pause.toMillis(), fatalFailures)
                    delay(pause.toMillis())
                } else {
                    fatalFailures = 0
                }
            }
        } finally {
            consumer = null
            try {
                kafka.close(Duration.ofSeconds(5))
            } catch (e: Exception) {
                log.debug("Error closing Kafka consumer", e)
            }
            log.info("Sync consumer stopped")
        }
    }

    /** @return false when the record could neither be applied nor dead-lettered (storage down). */
    private suspend fun process(record: ConsumerRecord<String, String>): Boolean {
        var attempt = 1
        while (true) {
            try {
                when (val outcome = handler.handle(record.value() ?: "")) {
                    is StockSyncHandler.Outcome.Applied -> applied.increment()
                    is StockSyncHandler.Outcome.Skipped -> {
                        skipped.increment()
                        log.info(
                            "Skipped sync record {}-{}@{}: {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            outcome.reason
                        )
                    }
                }
                return true
            } catch (e: CancellationException) {
                throw e
            } catch (e: StockSyncHandler.PoisonMessage) {
                return deadLetter(record, attempt, e)
            } catch (e: Exception) {
                if (attempt >= config.consumerMaxAttempts) return deadLetter(record, attempt, e)
                delay(Backoff.delayFor(attempt, config.consumerRetryBackoff, Duration.ofSeconds(2)).toMillis())
                attempt++
            }
        }
    }

    private suspend fun deadLetter(record: ConsumerRecord<String, String>, attempts: Int, cause: Exception): Boolean =
        try {
            val id = deadLetters.insert(
                NewDeadLetter(
                    source = DeadLetterSource.CONSUMER,
                    topic = record.topic(),
                    payload = record.value() ?: "",
                    messageKey = record.key(),
                    eventType = "STOCK_SYNC",
                    sourcePartition = record.partition(),
                    sourceOffset = record.offset(),
                    attempts = attempts,
                    error = cause.toString(),
                ),
            )
            deadLettered.increment()
            log.warn(
                "Sync record {}-{}@{} dead-lettered as #{}: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                id,
                cause.message
            )
            true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error(
                "Could not dead-letter sync record {}-{}@{}",
                record.topic(),
                record.partition(),
                record.offset(),
                e
            )
            false
        }

    private fun commit(kafka: KafkaConsumer<String, String>, partition: TopicPartition, nextOffset: Long) {
        try {
            kafka.commitSync(mapOf(partition to OffsetAndMetadata(nextOffset)))
        } catch (e: CommitFailedException) {
            log.warn("Offset commit failed for {} (rebalance); records will be redelivered and deduplicated", partition)
        } catch (e: WakeupException) {
            // Shutdown requested mid-commit: the wakeup flag is cleared, so finish the commit and let the loop exit.
            kafka.commitSync(mapOf(partition to OffsetAndMetadata(nextOffset)))
        }
    }
}
