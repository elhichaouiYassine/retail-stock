package com.retail.stock.api

import com.retail.stock.api.config.AppConfig
import com.retail.stock.domain.service.StockService
import com.retail.stock.infrastructure.db.Database
import com.retail.stock.infrastructure.db.MaintenanceRepository
import com.retail.stock.infrastructure.db.PostgresStockStore
import com.retail.stock.infrastructure.dlq.DeadLetterRepository
import com.retail.stock.infrastructure.dlq.DeadLetterService
import com.retail.stock.infrastructure.jobs.BackgroundJobs
import com.retail.stock.infrastructure.kafka.KafkaClients
import com.retail.stock.infrastructure.kafka.StockSyncConsumer
import com.retail.stock.infrastructure.kafka.StockSyncHandler
import com.retail.stock.infrastructure.outbox.OutboxRelay
import com.retail.stock.infrastructure.outbox.OutboxRepository
import com.retail.stock.infrastructure.outbox.OutboxSignal
import com.retail.stock.infrastructure.serialization.JsonOperationResultCodec
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration

/**
 * Composition root: builds every adapter once and owns their lifecycle.
 * Background work (outbox relay, Kafka consumer, sweepers) runs in a scope supplied by the Ktor application,
 * so it is cancelled with the application (structured concurrency).
 */
class RetailStockRuntime(val config: AppConfig, meterRegistry: MeterRegistry) : AutoCloseable {
    private val log = LoggerFactory.getLogger(RetailStockRuntime::class.java)

    val database = Database(config.database, meterRegistry)
    private val outboxSignal = OutboxSignal()
    private val store = PostgresStockStore(database, config.kafka.eventsTopic, outboxSignal)

    val stockService = StockService(
        transactions = store,
        queries = store,
        codec = JsonOperationResultCodec,
        clock = Clock.systemUTC(),
        policy = config.stockPolicy,
    )

    private val outboxRepository = OutboxRepository(database)
    private val deadLetterRepository = DeadLetterRepository(database)
    private val syncHandler = StockSyncHandler(stockService)
    val deadLetterService = DeadLetterService(deadLetterRepository, syncHandler)

    private val producer = KafkaClients.producer(config.kafka)
    private val producerMetrics = KafkaClientMetrics(producer).also { it.bindTo(meterRegistry) }

    val relay = OutboxRelay(outboxRepository, producer, outboxSignal, config.outbox, config.instanceId, meterRegistry)
    private val consumer =
        StockSyncConsumer(config.kafka, syncHandler, deadLetterRepository, config.instanceId, meterRegistry)
    private val jobs = BackgroundJobs(
        config.jobs,
        stockService,
        MaintenanceRepository(database),
        outboxRepository,
        deadLetterRepository,
        meterRegistry,
    )

    fun start(scope: CoroutineScope) {
        database.migrate()
        if (config.kafka.autoCreateTopics) {
            scope.launch(Dispatchers.IO) { KafkaClients.ensureTopics(config.kafka) }
        }
        relay.start(scope)
        if (config.kafka.consumerEnabled) consumer.start(scope)
        jobs.start(scope)
        log.info("Retail Stock runtime started (instance={}, env={})", config.instanceId, config.environment)
    }

    /** Unblocks the Kafka poll thread so the consumer job can be cancelled promptly. */
    fun beginShutdown() {
        consumer.wakeup()
    }

    override fun close() {
        runCatching { producerMetrics.close() }
        // Flushes in-flight records; anything unacknowledged stays leased in the outbox and is re-sent later.
        runCatching { producer.close(Duration.ofSeconds(10)) }.onFailure { log.warn("Kafka producer close failed", it) }
        runCatching { database.close() }.onFailure { log.warn("Database close failed", it) }
        log.info("Retail Stock runtime closed")
    }
}
