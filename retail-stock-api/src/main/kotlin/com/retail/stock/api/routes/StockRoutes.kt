package com.retail.stock.api.routes

import com.retail.stock.api.dto.*
import com.retail.stock.api.security.StockPrincipal
import com.retail.stock.api.security.asActor
import com.retail.stock.api.security.secured
import com.retail.stock.domain.command.*
import com.retail.stock.domain.error.AccessDenied
import com.retail.stock.domain.error.ValidationFailed
import com.retail.stock.domain.model.*
import com.retail.stock.domain.security.Permission
import com.retail.stock.domain.service.StockService
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

const val IDEMPOTENCY_HEADER = "Idempotency-Key"
private const val LOCATION = "locationId"

fun Route.meRoutes() {
    get("/me") {
        val actor = call.principal<StockPrincipal>()?.actor ?: throw AccessDenied("Authentication required")
        call.respond(actor.toResponse())
    }
}

fun Route.stockRoutes(service: StockService) {
    secured(HttpMethod.Get, "/stock/{sku}", Permission.STOCK_READ) {
        val sku = Sku(call.pathParam("sku"))
        val limit = call.queryInt("limit", 100)
        val levels = asActor { service.getLevelsBySku(sku, limit) }
        call.respond(StockLevelsResponse(levels.map { it.toResponse() }))
    }

    secured(
        HttpMethod.Get,
        "/stock/{sku}/locations/{locationId}",
        Permission.STOCK_READ,
        locationParameter = LOCATION
    ) {
        val level = asActor { service.getLevel(call.stockKey()) }
        call.respond(level.toResponse())
    }

    secured(
        HttpMethod.Get,
        "/stock/{sku}/locations/{locationId}/movements",
        Permission.STOCK_READ,
        locationParameter = LOCATION
    ) {
        val limit = call.queryInt("limit", 50)
        val before = call.request.queryParameters["before"]?.let { parseInstant("before", it) }
        val movements = asActor { service.getMovements(call.stockKey(), limit, before) }
        call.respond(
            MovementsResponse(
                items = movements.map { it.toResponse() },
                nextBefore = movements.takeIf { it.size >= limit }?.lastOrNull()?.occurredAt?.toString(),
            ),
        )
    }

    secured(
        HttpMethod.Post,
        "/stock/{sku}/locations/{locationId}/adjustments",
        Permission.STOCK_WRITE,
        locationParameter = LOCATION
    ) {
        val body = call.receive<AdjustStockRequest>()
        val level = asActor {
            service.adjust(AdjustStock(call.stockKey(), body.delta, body.reason, body.reference, call.idempotencyKey()))
        }
        call.respond(level.toResponse())
    }

    secured(
        HttpMethod.Post,
        "/stock/{sku}/locations/{locationId}/sync",
        Permission.STOCK_SYNC,
        locationParameter = LOCATION
    ) {
        val body = call.receive<SyncStockRequest>()
        val level = asActor {
            service.sync(
                SyncStock(
                    key = call.stockKey(),
                    countedOnHand = body.onHand,
                    source = body.source,
                    countedAt = body.countedAt?.let { parseInstant("countedAt", it) },
                    idempotencyKey = call.idempotencyKey(),
                ),
            )
        }
        call.respond(level.toResponse())
    }

    secured(HttpMethod.Post, "/batches/adjustments", Permission.STOCK_WRITE) {
        val body = call.receive<BatchAdjustRequest>()
        val lines = body.lines.map {
            AdjustmentLine(
                StockKey(Sku(it.sku), LocationId(it.locationId)),
                it.delta,
                it.reason,
                it.reference
            )
        }
        val levels = asActor { service.adjustBatch(BatchAdjustStock(lines, call.idempotencyKey())) }
        call.respond(StockLevelsResponse(levels.map { it.toResponse() }))
    }
}

fun Route.reservationRoutes(service: StockService) {
    secured(HttpMethod.Post, "/reservations", Permission.STOCK_RESERVE) {
        val body = call.receive<ReserveStockRequest>()
        val outcome = asActor {
            service.reserve(
                ReserveStock(
                    key = StockKey(Sku(body.sku), LocationId(body.locationId)),
                    quantity = body.quantity,
                    ttl = body.ttlSeconds?.let(Duration::ofSeconds),
                    reference = body.reference,
                    idempotencyKey = call.idempotencyKey(),
                ),
            )
        }
        call.respond(HttpStatusCode.Created, outcome.reservation.toResponse(outcome.level))
    }

    secured(HttpMethod.Get, "/reservations/{reservationId}", Permission.STOCK_READ) {
        val reservation = asActor { service.getReservation(ReservationId.parse(call.pathParam("reservationId"))) }
        call.respond(reservation.toResponse())
    }

    secured(HttpMethod.Post, "/reservations/{reservationId}/commit", Permission.STOCK_RESERVE) {
        val id = ReservationId.parse(call.pathParam("reservationId"))
        val outcome = asActor { service.commit(CommitReservation(id, call.idempotencyKey())) }
        call.respond(outcome.reservation.toResponse(outcome.level))
    }

    secured(HttpMethod.Post, "/reservations/{reservationId}/release", Permission.STOCK_RESERVE) {
        val id = ReservationId.parse(call.pathParam("reservationId"))
        val body = call.receiveReleaseBody()
        val outcome = asActor { service.release(ReleaseReservation(id, body?.reason, call.idempotencyKey())) }
        call.respond(outcome.reservation.toResponse(outcome.level))
    }
}

// ------------------------------------------------------------------ helpers

internal fun ApplicationCall.pathParam(name: String): String =
    parameters[name]?.takeIf(String::isNotBlank) ?: throw ValidationFailed("Missing path parameter '$name'")

private fun ApplicationCall.stockKey(): StockKey = StockKey(Sku(pathParam("sku")), LocationId(pathParam(LOCATION)))

/** Mutations must be retry-safe: clients (and our own load balancer retries) resend with the same key. */
private fun ApplicationCall.idempotencyKey(): IdempotencyKey {
    val raw = request.header(IDEMPOTENCY_HEADER)
        ?: throw ValidationFailed("Header '$IDEMPOTENCY_HEADER' is required for mutations")
    return IdempotencyKey(raw.trim())
}

internal fun ApplicationCall.queryInt(name: String, default: Int): Int =
    request.queryParameters[name]?.let {
        it.toIntOrNull() ?: throw ValidationFailed("Query parameter '$name' must be an integer")
    }
        ?: default

private fun parseInstant(field: String, raw: String): Instant = try {
    Instant.parse(raw)
} catch (e: DateTimeParseException) {
    throw ValidationFailed("'$field' must be an ISO-8601 instant, e.g. 2026-11-04T10:15:30Z")
}

private suspend fun ApplicationCall.receiveReleaseBody(): ReleaseReservationRequest? {
    val length = request.header("Content-Length")?.toLongOrNull()
    if (length == 0L || (length == null && request.header("Transfer-Encoding") == null)) return null
    return receive<ReleaseReservationRequest>()
}
