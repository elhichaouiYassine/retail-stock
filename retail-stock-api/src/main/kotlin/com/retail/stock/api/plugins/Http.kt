package com.retail.stock.api.plugins

import com.retail.stock.api.config.HttpConfig
import com.retail.stock.api.dto.ErrorResponse
import com.retail.stock.domain.error.*
import com.retail.stock.infrastructure.db.Database
import com.retail.stock.infrastructure.serialization.StockJson
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.plugins.*
import io.ktor.server.plugins.ContentTransformationException
import io.ktor.server.plugins.callid.*
import io.ktor.server.plugins.calllogging.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.defaultheaders.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import kotlinx.serialization.SerializationException
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.sql.SQLException
import java.sql.SQLTransientConnectionException
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

private val log = LoggerFactory.getLogger("com.retail.stock.api.http")

fun Application.configureHttp(config: HttpConfig, meterRegistry: MeterRegistry) {
    install(DefaultHeaders) {
        header("X-Content-Type-Options", "nosniff")
        header("Cache-Control", "no-store")
    }
    install(CallId) {
        header(HttpHeaders.XRequestId)
        generate { UUID.randomUUID().toString() }
        verify { it.isNotBlank() && it.length <= 128 }
    }
    install(CallLogging) {
        level = Level.INFO
        callIdMdc("traceId")
        filter { call -> call.request.path().startsWith("/api") }
    }
    install(ContentNegotiation) {
        json(StockJson)
    }
    install(MicrometerMetrics) {
        registry = meterRegistry
        meterBinders = listOf(
            JvmMemoryMetrics(),
            JvmGcMetrics(),
            JvmThreadMetrics(),
            ClassLoaderMetrics(),
            ProcessorMetrics(),
            UptimeMetrics(),
        )
    }
    configureErrorHandling()
    configureLoadShedding(config, meterRegistry)
}

/**
 * In-flight request gauge (the custom HPA signal, scraped by Datadog as `retail_stock.http.requests_in_flight`)
 * and edge backpressure: above [HttpConfig.maxConcurrentRequests] calls are rejected immediately with
 * 503 + Retry-After instead of queueing behind hot-row locks and exhausting the DB pool.
 */
private fun Application.configureLoadShedding(config: HttpConfig, meterRegistry: MeterRegistry) {
    val inFlight = AtomicInteger(0)
    meterRegistry.gauge("retail.stock.http.requests.in_flight", inFlight)
    val shed = Counter.builder("retail.stock.http.requests.shed").register(meterRegistry)

    intercept(ApplicationCallPipeline.Monitoring) {
        val path = call.request.path()
        if (!path.startsWith("/api/")) {
            proceed()
            return@intercept
        }
        val current = inFlight.incrementAndGet()
        try {
            if (current > config.maxConcurrentRequests) {
                shed.increment()
                call.response.header(HttpHeaders.RetryAfter, "1")
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    ErrorResponse("OVERLOADED", "Too many concurrent requests, retry shortly", traceId = call.callId),
                )
                finish()
            } else {
                proceed()
            }
        } finally {
            inFlight.decrementAndGet()
        }
    }
}

private fun Application.configureErrorHandling() {
    install(StatusPages) {
        exception<StockException> { call, cause ->
            call.respondError(statusFor(cause), cause.code, cause.message ?: cause.code, cause.details)
        }
        exception<BadRequestException> { call, cause ->
            call.respondError(HttpStatusCode.BadRequest, "BAD_REQUEST", cause.rootMessage())
        }
        exception<SerializationException> { call, cause ->
            call.respondError(HttpStatusCode.BadRequest, "BAD_REQUEST", cause.message ?: "Malformed JSON")
        }
        exception<UnsupportedMediaTypeException> { call, cause ->
            call.respondError(
                HttpStatusCode.UnsupportedMediaType,
                "UNSUPPORTED_MEDIA_TYPE",
                cause.message ?: "Use application/json"
            )
        }
        exception<ContentTransformationException> { call, cause ->
            call.respondError(
                HttpStatusCode.BadRequest,
                "BAD_REQUEST",
                cause.message ?: "Request body could not be read"
            )
        }
        exception<PayloadTooLargeException> { call, cause ->
            call.respondError(HttpStatusCode.PayloadTooLarge, "PAYLOAD_TOO_LARGE", cause.message ?: "Payload too large")
        }
        exception<NotFoundException> { call, cause ->
            call.respondError(HttpStatusCode.NotFound, "NOT_FOUND", cause.message ?: "Not found")
        }
        exception<SQLException> { call, cause ->
            val busy = cause.sqlState == Database.LOCK_NOT_AVAILABLE || cause.sqlState == Database.QUERY_CANCELED
            if (busy) {
                // Hot SKU: lock_timeout / statement_timeout hit. Tell the client to retry (with the same Idempotency-Key).
                call.response.header(HttpHeaders.RetryAfter, "1")
                call.respondError(
                    HttpStatusCode.ServiceUnavailable,
                    "STOCK_BUSY",
                    "Stock item is busy, retry with the same Idempotency-Key"
                )
            } else if (cause is SQLTransientConnectionException) {
                call.response.header(HttpHeaders.RetryAfter, "2")
                call.respondError(
                    HttpStatusCode.ServiceUnavailable,
                    "DATABASE_UNAVAILABLE",
                    "Database temporarily unavailable"
                )
            } else {
                log.error("Database error [{}]", cause.sqlState, cause)
                call.respondError(HttpStatusCode.InternalServerError, "INTERNAL_ERROR", "Unexpected database error")
            }
        }
        exception<Throwable> { call, cause ->
            log.error("Unhandled error on {} {}", call.request.httpMethod.value, call.request.path(), cause)
            call.respondError(HttpStatusCode.InternalServerError, "INTERNAL_ERROR", "Unexpected error")
        }
    }
}

private fun statusFor(e: StockException): HttpStatusCode = when (e) {
    is ValidationFailed -> HttpStatusCode.BadRequest
    is AccessDenied -> HttpStatusCode.Forbidden
    is StockLevelNotFound, is ReservationNotFound -> HttpStatusCode.NotFound
    is InsufficientStock, is ReservationNotActive, is StaleStockCount -> HttpStatusCode.Conflict
    is IdempotencyConflict -> HttpStatusCode.UnprocessableEntity
}

private suspend fun ApplicationCall.respondError(
    status: HttpStatusCode,
    code: String,
    message: String,
    details: Map<String, String> = emptyMap(),
) = respond(status, ErrorResponse(code, message, details, callId))

private fun Throwable.rootMessage(): String {
    var current: Throwable = this
    while (current.cause != null && current.cause !== current) current = current.cause!!
    return current.message ?: message ?: "Bad request"
}
