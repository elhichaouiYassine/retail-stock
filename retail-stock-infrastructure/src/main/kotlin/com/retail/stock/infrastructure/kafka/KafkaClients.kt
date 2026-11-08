package com.retail.stock.infrastructure.kafka

import com.retail.stock.infrastructure.config.KafkaConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

object KafkaClients {
    private val log = LoggerFactory.getLogger(KafkaClients::class.java)

    /**
     * Idempotent producer. With a single broker, `acks=all` is equivalent to the leader ack, but it
     * keeps the configuration correct the day replicas are added, and is required by idempotence.
     * `max.block.ms` is short so a dead broker surfaces as a fast failure the relay can back off on,
     * instead of blocking IO threads; durability is guaranteed upstream by the PostgreSQL outbox.
     */
    fun producer(config: KafkaConfig): KafkaProducer<String, String> = KafkaProducer(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.CLIENT_ID_CONFIG, "${config.clientId}-producer")
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
            put(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE)
            put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000)
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000)
            put(ProducerConfig.LINGER_MS_CONFIG, 5)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024)
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32L * 1024 * 1024)
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10_000)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        },
    )

    fun consumer(config: KafkaConfig, instanceId: String): KafkaConsumer<String, String> = KafkaConsumer(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ConsumerConfig.CLIENT_ID_CONFIG, "${config.clientId}-consumer-$instanceId")
            put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroupId)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000)
            put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10_000)
            put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
            )
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        },
    )

    /**
     * Creates the stock topics if missing (local docker-compose). In Kubernetes the Strimzi Topic Operator
     * owns topics through `KafkaTopic` resources and this is disabled. Never fatal: the outbox buffers events
     * while Kafka is unavailable.
     */
    fun ensureTopics(config: KafkaConfig) {
        val props = Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(AdminClientConfig.CLIENT_ID_CONFIG, "${config.clientId}-admin")
            put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
            put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000)
        }
        try {
            Admin.create(props).use { admin ->
                val existing = admin.listTopics().names().get(10, TimeUnit.SECONDS)
                val topicConfig = mapOf(
                    "min.insync.replicas" to "1",
                    "cleanup.policy" to "delete",
                    "retention.ms" to TimeUnit.DAYS.toMillis(7).toString(),
                    "compression.type" to "producer",
                )
                val missing = listOf(config.eventsTopic, config.syncTopic)
                    .filterNot { it in existing }
                    .map { NewTopic(it, config.topicPartitions, config.replicationFactor).configs(topicConfig) }
                if (missing.isNotEmpty()) {
                    admin.createTopics(missing).all().get(10, TimeUnit.SECONDS)
                    log.info("Created Kafka topics {}", missing.map { it.name() })
                }
            }
        } catch (e: Exception) {
            log.warn(
                "Kafka topic provisioning skipped ({}). Events stay buffered in the outbox until Kafka is reachable.",
                e.toString()
            )
        }
    }
}

/**
 * Non-blocking hand-off to the producer: `send` only appends to the in-memory accumulator, preserving
 * per-partition order when called sequentially. Completion is observed through the returned [Deferred].
 * Must be called from [kotlinx.coroutines.Dispatchers.IO]: `send` may block up to `max.block.ms`.
 */
fun <K, V> Producer<K, V>.dispatch(record: ProducerRecord<K, V>): Deferred<RecordMetadata> {
    val result = CompletableDeferred<RecordMetadata>()
    try {
        send(record) { metadata, exception ->
            if (exception != null) result.completeExceptionally(exception) else result.complete(metadata)
        }
    } catch (e: Exception) {
        result.completeExceptionally(e)
    }
    return result
}
