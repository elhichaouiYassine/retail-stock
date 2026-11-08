package com.retail.stock.api.routes

import com.retail.stock.api.dto.HealthResponse
import com.retail.stock.infrastructure.db.Database
import com.retail.stock.infrastructure.outbox.OutboxRelay
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

private val PROMETHEUS_CONTENT_TYPE = ContentType.parse("text/plain; version=0.0.4; charset=utf-8")

/** Unauthenticated endpoints: health probes and Prometheus metrics. The service is API only, no UI. */
fun Route.systemRoutes(
    database: Database,
    relay: OutboxRelay,
    registry: PrometheusMeterRegistry,
) {
    get("/health/live") {
        call.respond(HealthResponse("UP", emptyMap()))
    }

    // Readiness depends on PostgreSQL only. Kafka being down must NOT take pods out of rotation:
    // writes keep flowing into the outbox and are published when the broker returns.
    get("/health/ready") {
        val dbUp = database.isHealthy()
        val checks = mapOf(
            "postgres" to if (dbUp) "UP" else "DOWN",
            "kafka" to if (relay.healthy) "UP" else "DEGRADED (buffering in outbox)",
        )
        call.respond(
            if (dbUp) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable,
            HealthResponse(if (dbUp) "UP" else "DOWN", checks)
        )
    }

    get("/metrics") {
        call.respondText(registry.scrape(), PROMETHEUS_CONTENT_TYPE)
    }
}
