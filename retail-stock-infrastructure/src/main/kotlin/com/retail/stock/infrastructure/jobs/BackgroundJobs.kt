package com.retail.stock.infrastructure.jobs

import com.retail.stock.domain.security.Actor
import com.retail.stock.domain.security.ActorContext
import com.retail.stock.domain.service.StockService
import com.retail.stock.infrastructure.config.JobsConfig
import com.retail.stock.infrastructure.db.MaintenanceRepository
import com.retail.stock.infrastructure.dlq.DeadLetterRepository
import com.retail.stock.infrastructure.outbox.OutboxRepository
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong

private val log = LoggerFactory.getLogger("com.retail.stock.infrastructure.jobs")

/** Runs [block] every [interval] (with up to 10% jitter) until the scope is cancelled. Failures are logged, never fatal. */
fun CoroutineScope.launchPeriodic(
    name: String,
    interval: Duration,
    initialDelay: Duration = Duration.ZERO,
    block: suspend () -> Unit,
): Job = launch(CoroutineName(name) + ActorContext(Actor.SYSTEM)) {
    delay(initialDelay.toMillis())
    while (isActive) {
        try {
            block()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warn("Background job '{}' failed: {}", name, e.toString())
        }
        val jitter = (interval.toMillis() * 0.1 * ThreadLocalRandom.current().nextDouble()).toLong()
        delay(interval.toMillis() + jitter)
    }
}

class BackgroundJobs(
    private val config: JobsConfig,
    private val stockService: StockService,
    private val maintenance: MaintenanceRepository,
    private val outbox: OutboxRepository,
    private val deadLetters: DeadLetterRepository,
    meterRegistry: MeterRegistry,
) {
    private val outboxPending = AtomicLong(0)
    private val outboxOldestAgeMillis = AtomicLong(0)
    private val deadLettersPending = AtomicLong(0)

    init {
        meterRegistry.gauge("retail.stock.outbox.pending", outboxPending)
        meterRegistry.gauge("retail.stock.outbox.oldest_age_ms", outboxOldestAgeMillis)
        meterRegistry.gauge("retail.stock.dead_letters.pending", deadLettersPending)
    }

    fun start(scope: CoroutineScope): List<Job> = listOf(
        scope.launchPeriodic("reservation-expiry", config.reservationExpiryInterval, Duration.ofSeconds(2)) {
            // Drain in batches until no expired reservation is left (SKIP LOCKED makes pods cooperate).
            while (stockService.expireReservations(config.reservationExpiryBatch) == config.reservationExpiryBatch) Unit
        },
        scope.launchPeriodic("partition-maintenance", config.maintenanceInterval, Duration.ofSeconds(10)) {
            val created = maintenance.ensureMovementPartitions(config.partitionMonthsAhead)
            if (created > 0) log.info("Created {} stock_movements partition(s)", created)
            val keys = maintenance.purgeIdempotencyKeys(config.idempotencyRetention)
            val reservations = maintenance.purgeClosedReservations(config.closedReservationRetention)
            if (keys + reservations > 0) log.info(
                "Purged {} idempotency keys and {} closed reservations",
                keys,
                reservations
            )
        },
        scope.launchPeriodic("backlog-metrics", config.metricsInterval) {
            val backlog = outbox.backlog()
            outboxPending.set(backlog.pending)
            outboxOldestAgeMillis.set((backlog.oldestAgeSeconds * 1000).toLong())
            deadLettersPending.set(deadLetters.countPending())
        },
    )
}
