package com.retail.stock.api.config

import com.retail.stock.domain.service.StockPolicy
import com.retail.stock.infrastructure.config.DatabaseConfig
import com.retail.stock.infrastructure.config.JobsConfig
import com.retail.stock.infrastructure.config.KafkaConfig
import com.retail.stock.infrastructure.config.OutboxConfig
import java.io.File
import java.time.Duration
import java.util.*

data class HttpConfig(
    val host: String,
    val port: Int,
    /** Load-shedding threshold per pod: beyond this many in-flight API calls, answer 503 + Retry-After. */
    val maxConcurrentRequests: Int,
)

data class SecurityConfig(
    /** `iss` expected in tokens: the URL clients use to reach Keycloak. */
    val issuer: String,
    /** Where this service fetches signing keys (may be a cluster-internal URL). */
    val jwksUrl: String,
    val audience: String,
    val leewaySeconds: Long,
    /** Teammate tokens are only accepted from these clients (the passkey client `retail-stock-web` in production). */
    val teammateClients: Set<String>,
)

data class AppConfig(
    val environment: String,
    val instanceId: String,
    val http: HttpConfig,
    val database: DatabaseConfig,
    val kafka: KafkaConfig,
    val outbox: OutboxConfig,
    val jobs: JobsConfig,
    val security: SecurityConfig,
    val stockPolicy: StockPolicy,
) {
    companion object {
        /**
         * Resolution order: process environment > `.env` file (searched upward from the working directory) >
         * built-in defaults. The defaults match the generated `.env` and docker-compose, so the application
         * starts with zero configuration on a developer machine.
         */
        fun load(
            environment: Map<String, String> = System.getenv(),
            dotEnv: Map<String, String> = DotEnv.load(),
        ): AppConfig {
            val env = Env(listOf(environment, dotEnv))
            val instanceId = env.string("HOSTNAME", "local-" + UUID.randomUUID().toString().take(8))
            val issuer = env.string("OIDC_ISSUER", "http://localhost:8180/realms/retail-stock").trimEnd('/')

            return AppConfig(
                environment = env.string("APP_ENV", "local"),
                instanceId = instanceId,
                http = HttpConfig(
                    host = env.string("APP_HOST", "0.0.0.0"),
                    port = env.int("APP_PORT", 8080),
                    maxConcurrentRequests = env.int("APP_MAX_CONCURRENT_REQUESTS", 512),
                ),
                database = DatabaseConfig(
                    url = env.string("DB_URL", "jdbc:postgresql://localhost:5433/retail_stock"),
                    username = env.string("DB_USER", "retail_stock"),
                    password = env.string("DB_PASSWORD", "retail_stock_pwd"),
                    maxPoolSize = env.int("DB_POOL_MAX", 20),
                    minIdle = env.int("DB_POOL_MIN_IDLE", 5),
                    statementTimeout = env.duration("DB_STATEMENT_TIMEOUT", Duration.ofSeconds(5)),
                    lockTimeout = env.duration("DB_LOCK_TIMEOUT", Duration.ofSeconds(2)),
                ),
                kafka = KafkaConfig(
                    bootstrapServers = env.string("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    clientId = env.string("KAFKA_CLIENT_ID", "retail-stock"),
                    eventsTopic = env.string("KAFKA_EVENTS_TOPIC", "retail.stock.events.v1"),
                    syncTopic = env.string("KAFKA_SYNC_TOPIC", "retail.stock.sync.v1"),
                    consumerGroupId = env.string("KAFKA_CONSUMER_GROUP", "retail-stock-sync"),
                    topicPartitions = env.int("KAFKA_TOPIC_PARTITIONS", 12),
                    autoCreateTopics = env.bool("KAFKA_AUTO_CREATE_TOPICS", true),
                    consumerEnabled = env.bool("KAFKA_CONSUMER_ENABLED", true),
                ),
                outbox = OutboxConfig(
                    batchSize = env.int("OUTBOX_BATCH_SIZE", 500),
                    pollInterval = env.duration("OUTBOX_POLL_INTERVAL", Duration.ofMillis(200)),
                    maxAttempts = env.int("OUTBOX_MAX_ATTEMPTS", 20),
                ),
                jobs = JobsConfig(
                    reservationExpiryInterval = env.duration("JOBS_RESERVATION_EXPIRY_INTERVAL", Duration.ofSeconds(5)),
                    idempotencyRetention = env.duration("JOBS_IDEMPOTENCY_RETENTION", Duration.ofHours(48)),
                ),
                security = SecurityConfig(
                    issuer = issuer,
                    jwksUrl = env.string("OIDC_JWKS_URL", "$issuer/protocol/openid-connect/certs"),
                    audience = env.string("OIDC_AUDIENCE", "retail-stock-api"),
                    leewaySeconds = env.long("OIDC_LEEWAY_SECONDS", 5),
                    teammateClients = env.list(
                        "OIDC_TEAMMATE_CLIENTS",
                        listOf("retail-stock-web", "retail-stock-dev-cli")
                    ).toSet(),
                ),
                stockPolicy = StockPolicy(
                    defaultReservationTtl = env.duration("STOCK_RESERVATION_DEFAULT_TTL", Duration.ofMinutes(15)),
                    maxReservationTtl = env.duration("STOCK_RESERVATION_MAX_TTL", Duration.ofHours(24)),
                    maxBatchSize = env.int("STOCK_MAX_BATCH_SIZE", 500),
                ),
            )
        }
    }
}

class Env(private val sources: List<Map<String, String>>) {
    fun raw(name: String): String? = sources.firstNotNullOfOrNull { it[name]?.takeIf(String::isNotBlank) }

    fun string(name: String, default: String): String = raw(name) ?: default

    fun int(name: String, default: Int): Int = raw(name)?.let {
        it.toIntOrNull() ?: throw IllegalArgumentException("$name must be an integer, was '$it'")
    } ?: default

    fun long(name: String, default: Long): Long = raw(name)?.let {
        it.toLongOrNull() ?: throw IllegalArgumentException("$name must be a long, was '$it'")
    } ?: default

    fun bool(name: String, default: Boolean): Boolean = raw(name)?.let {
        when (it.lowercase()) {
            "true", "1", "yes", "on" -> true
            "false", "0", "no", "off" -> false
            else -> throw IllegalArgumentException("$name must be a boolean, was '$it'")
        }
    } ?: default

    fun list(name: String, default: List<String>): List<String> =
        raw(name)?.split(',')?.map(String::trim)?.filter(String::isNotEmpty) ?: default

    /** Accepts ISO-8601 (`PT5S`) or shorthand: `250ms`, `5s`, `2m`, `1h`, `7d`. */
    fun duration(name: String, default: Duration): Duration = raw(name)?.let { parseDuration(name, it) } ?: default

    private fun parseDuration(name: String, value: String): Duration {
        val v = value.trim().lowercase()
        return try {
            when {
                v.startsWith("p") -> Duration.parse(value.trim().uppercase())
                v.endsWith("ms") -> Duration.ofMillis(v.removeSuffix("ms").toLong())
                v.endsWith("s") -> Duration.ofSeconds(v.removeSuffix("s").toLong())
                v.endsWith("m") -> Duration.ofMinutes(v.removeSuffix("m").toLong())
                v.endsWith("h") -> Duration.ofHours(v.removeSuffix("h").toLong())
                v.endsWith("d") -> Duration.ofDays(v.removeSuffix("d").toLong())
                else -> Duration.ofMillis(v.toLong())
            }
        } catch (e: Exception) {
            throw IllegalArgumentException("$name must be a duration (e.g. 500ms, 5s, PT1M), was '$value'", e)
        }
    }
}

/** Minimal `.env` reader (KEY=VALUE, `#` comments, optional quotes and `export` prefix). */
object DotEnv {
    fun load(startDirectory: File = File(System.getProperty("user.dir"))): Map<String, String> {
        var dir: File? = startDirectory.absoluteFile
        repeat(3) {
            val candidate = File(dir, ".env")
            if (candidate.isFile) return parse(candidate.readLines())
            dir = dir?.parentFile
        }
        return emptyMap()
    }

    fun parse(lines: List<String>): Map<String, String> = lines.asSequence()
        .map(String::trim)
        .filter { it.isNotEmpty() && !it.startsWith("#") && it.contains('=') }
        .map { it.removePrefix("export ").trim() }
        .associate { line ->
            val key = line.substringBefore('=').trim()
            var value = line.substringAfter('=').trim()
            if (value.length >= 2 && (value.first() == '"' && value.last() == '"' || value.first() == '\'' && value.last() == '\'')) {
                value = value.substring(1, value.length - 1)
            } else {
                value = value.substringBefore(" #").trim()
            }
            key to value
        }
}
