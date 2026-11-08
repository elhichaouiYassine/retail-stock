package com.retail.stock.domain.error

/**
 * Business failures. Stack traces are disabled: these are expected, frequent outcomes
 * (e.g. insufficient stock during a flash sale) and capturing traces at 1k ops/s is pure GC pressure.
 */
sealed class StockException(
    val code: String,
    message: String,
    val details: Map<String, String> = emptyMap(),
) : RuntimeException(message, null, false, false)

class ValidationFailed(message: String, details: Map<String, String> = emptyMap()) :
    StockException("VALIDATION_FAILED", message, details)

class AccessDenied(message: String) : StockException("ACCESS_DENIED", message)

class InsufficientStock(sku: String, locationId: String, requested: Long, available: Long) :
    StockException(
        code = "INSUFFICIENT_STOCK",
        message = "Insufficient stock for $sku at $locationId: requested $requested, available $available",
        details = mapOf(
            "sku" to sku,
            "locationId" to locationId,
            "requested" to requested.toString(),
            "available" to available.toString(),
        ),
    )

class StockLevelNotFound(sku: String, locationId: String) :
    StockException(
        "STOCK_NOT_FOUND",
        "No stock record for $sku at $locationId",
        mapOf("sku" to sku, "locationId" to locationId)
    )

class ReservationNotFound(reservationId: String) :
    StockException(
        "RESERVATION_NOT_FOUND",
        "Reservation $reservationId not found",
        mapOf("reservationId" to reservationId)
    )

class ReservationNotActive(reservationId: String, status: String) :
    StockException(
        "RESERVATION_NOT_ACTIVE",
        "Reservation $reservationId is $status",
        mapOf("reservationId" to reservationId, "status" to status),
    )

class StaleStockCount(sku: String, locationId: String, countedAt: String, lastUpdatedAt: String) :
    StockException(
        "STALE_STOCK_COUNT",
        "Count for $sku at $locationId taken at $countedAt is older than last update $lastUpdatedAt",
        mapOf("sku" to sku, "locationId" to locationId, "countedAt" to countedAt, "lastUpdatedAt" to lastUpdatedAt),
    )

class IdempotencyConflict(key: String) :
    StockException(
        "IDEMPOTENCY_CONFLICT",
        "Idempotency key '$key' was already used with a different payload",
        mapOf("idempotencyKey" to key),
    )
