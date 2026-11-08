package com.retail.stock.infrastructure.db

import com.retail.stock.infrastructure.config.DatabaseConfig
import com.retail.stock.infrastructure.resilience.Backoff
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration

/**
 * HikariCP pool + a coroutine dispatcher whose parallelism equals the pool size.
 *
 * JDBC is blocking, so every call is confined to [Dispatchers.IO] (limited to `maxPoolSize` threads):
 * coroutines beyond that suspend in the dispatcher queue instead of parking threads on `getConnection()`,
 * which keeps the Netty event loops and the Default dispatcher free under burst load.
 */
class Database(private val config: DatabaseConfig, meterRegistry: MeterRegistry?) : AutoCloseable {
    private val log = LoggerFactory.getLogger(Database::class.java)

    val dataSource: HikariDataSource = HikariDataSource(
        HikariConfig().apply {
            poolName = "retail-stock-pg"
            jdbcUrl = config.url
            username = config.username
            password = config.password
            maximumPoolSize = config.maxPoolSize
            minimumIdle = config.minIdle.coerceAtMost(config.maxPoolSize)
            connectionTimeout = config.connectionTimeout.toMillis()
            validationTimeout = 2_000
            idleTimeout = Duration.ofMinutes(5).toMillis()
            maxLifetime = Duration.ofMinutes(30).toMillis()
            keepaliveTime = Duration.ofMinutes(2).toMillis()
            isAutoCommit = false
            transactionIsolation = "TRANSACTION_READ_COMMITTED"
            // Fail fast if PostgreSQL is not reachable at boot rather than serving 500s.
            initializationFailTimeout = Duration.ofSeconds(30).toMillis()
            meterRegistry?.let { metricRegistry = it }
            addDataSourceProperty("ApplicationName", "retail-stock")
            addDataSourceProperty("reWriteBatchedInserts", "true")
            addDataSourceProperty("tcpKeepAlive", "true")
            addDataSourceProperty("prepareThreshold", "3")
            addDataSourceProperty(
                "options",
                "-c statement_timeout=${config.statementTimeout.toMillis()} " +
                        "-c lock_timeout=${config.lockTimeout.toMillis()} " +
                        "-c idle_in_transaction_session_timeout=${config.idleInTransactionTimeout.toMillis()}",
            )
        },
    )

    @OptIn(ExperimentalCoroutinesApi::class)
    val dispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(config.maxPoolSize)

    /** Runs Flyway on a dedicated connection without statement timeouts (DDL may be slow on big tables). */
    fun migrate() {
        val result = Flyway.configure()
            .dataSource(config.url, config.username, config.password)
            .locations("classpath:db/migration")
            .connectRetries(10)
            .load()
            .migrate()
        log.info(
            "Database migrated: {} migration(s) applied, schema version {}",
            result.migrationsExecuted,
            result.targetSchemaVersion
        )
    }

    /**
     * Executes [block] in one transaction. Serialization failures and deadlocks are retried with
     * jittered backoff; the block must therefore be free of side effects outside the transaction.
     */
    suspend fun <T> transaction(readOnly: Boolean = false, block: suspend (Connection) -> T): T {
        var attempt = 1
        while (true) {
            try {
                return withContext(dispatcher) { runInTransaction(readOnly, block) }
            } catch (e: SQLException) {
                if (attempt >= config.maxTransactionRetries || !e.isRetryable()) throw e
                log.debug("Retrying transaction after {} (attempt {})", e.sqlState, attempt)
                delay(Backoff.delayFor(attempt, Duration.ofMillis(10), Duration.ofMillis(200)).toMillis())
                attempt++
            }
        }
    }

    private suspend fun <T> runInTransaction(readOnly: Boolean, block: suspend (Connection) -> T): T =
        dataSource.connection.use { connection ->
            connection.isReadOnly = readOnly
            try {
                val result = block(connection)
                connection.commit()
                result
            } catch (e: Throwable) {
                try {
                    connection.rollback()
                } catch (rollbackFailure: SQLException) {
                    e.addSuppressed(rollbackFailure)
                }
                throw e
            } finally {
                if (readOnly) connection.isReadOnly = false
            }
        }

    suspend fun isHealthy(): Boolean = try {
        transaction(readOnly = true) { c -> c.prepareStatement("SELECT 1").use { it.executeQuery().next() } }
    } catch (e: SQLException) {
        log.warn("Database health check failed: {}", e.message)
        false
    }

    override fun close() {
        dataSource.close()
    }

    private fun SQLException.isRetryable(): Boolean =
        sqlState == SERIALIZATION_FAILURE || sqlState == DEADLOCK_DETECTED

    companion object {
        const val SERIALIZATION_FAILURE = "40001"
        const val DEADLOCK_DETECTED = "40P01"
        const val UNIQUE_VIOLATION = "23505"
        const val LOCK_NOT_AVAILABLE = "55P03"
        const val QUERY_CANCELED = "57014"
    }
}
