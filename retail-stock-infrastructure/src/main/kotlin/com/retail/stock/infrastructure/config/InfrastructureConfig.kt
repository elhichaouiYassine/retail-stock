package com.retail.stock.infrastructure.config

import java.time.Duration

data class DatabaseConfig(
    val url: String,
    val username: String,
    val password: String,
    /** Per pod. 10 pods x 20 = 200 server connections; front with PgBouncer beyond that. */
    val maxPoolSize: Int = 20,
    val minIdle: Int = 5,
    val connectionTimeout: Duration = Duration.ofSeconds(3),
    val statementTimeout: Duration = Duration.ofSeconds(5),
    /** Bounded wait on hot-row locks: fail fast instead of piling up requests behind a hot SKU. */
    val lockTimeout: Duration = Duration.ofSeconds(2),
    val idleInTransactionTimeout: Duration = Duration.ofSeconds(15),
    val maxTransactionRetries: Int = 3,
) {
    override fun toString(): String =
        "DatabaseConfig(url=$url, username=$username, password=***, maxPoolSize=$maxPoolSize, minIdle=$minIdle)"
}

data class KafkaConfig(
    val bootstrapServers: String,
    val clientId: String,
    val eventsTopic: String = "retail.stock.events.v1",
    val syncTopic: String = "retail.stock.sync.v1",
    val consumerGroupId: String = "retail-stock-sync",
    val topicPartitions: Int = 12,
    /** Single broker cluster: replication factor is necessarily 1. */
    val replicationFactor: Short = 1,
    val autoCreateTopics: Boolean = true,
    val consumerEnabled: Boolean = true,
    val consumerMaxAttempts: Int = 3,
    val consumerRetryBackoff: Duration = Duration.ofMillis(250),
    val maxPollRecords: Int = 200,
)

data class OutboxConfig(
    val batchSize: Int = 500,
    val pollInterval: Duration = Duration.ofMillis(200),
    val lease: Duration = Duration.ofSeconds(60),
    /** In-memory buffer between DB poller and Kafka publisher (backpressure boundary). */
    val bufferedBatches: Int = 4,
    val maxAttempts: Int = 20,
    val baseBackoff: Duration = Duration.ofMillis(500),
    val maxBackoff: Duration = Duration.ofMinutes(5),
    val sendTimeout: Duration = Duration.ofSeconds(35),
)

data class JobsConfig(
    val reservationExpiryInterval: Duration = Duration.ofSeconds(5),
    val reservationExpiryBatch: Int = 500,
    val maintenanceInterval: Duration = Duration.ofHours(1),
    val idempotencyRetention: Duration = Duration.ofHours(48),
    val closedReservationRetention: Duration = Duration.ofDays(30),
    val partitionMonthsAhead: Int = 2,
    val metricsInterval: Duration = Duration.ofSeconds(15),
)
