package com.retail.stock.api.dto

import com.retail.stock.domain.model.Reservation
import com.retail.stock.domain.model.StockLevel
import com.retail.stock.domain.model.StockMovement
import com.retail.stock.domain.security.Actor
import com.retail.stock.domain.security.LocationScope
import com.retail.stock.infrastructure.dlq.DeadLetter
import kotlinx.serialization.Serializable

@Serializable
data class AdjustStockRequest(
    val delta: Long,
    val reason: String,
    val reference: String? = null,
)

@Serializable
data class SyncStockRequest(
    val onHand: Long,
    val source: String,
    val countedAt: String? = null,
)

@Serializable
data class BatchAdjustmentLine(
    val sku: String,
    val locationId: String,
    val delta: Long,
    val reason: String,
    val reference: String? = null,
)

@Serializable
data class BatchAdjustRequest(val lines: List<BatchAdjustmentLine>)

@Serializable
data class ReserveStockRequest(
    val sku: String,
    val locationId: String,
    val quantity: Long,
    val ttlSeconds: Long? = null,
    val reference: String? = null,
)

@Serializable
data class ReleaseReservationRequest(val reason: String? = null)

// ------------------------------------------------------------------ responses

@Serializable
data class StockLevelResponse(
    val sku: String,
    val locationId: String,
    val onHand: Long,
    val reserved: Long,
    val available: Long,
    val oversold: Boolean,
    val version: Long,
    val updatedAt: String,
)

@Serializable
data class StockLevelsResponse(val items: List<StockLevelResponse>)

@Serializable
data class ReservationResponse(
    val id: String,
    val sku: String,
    val locationId: String,
    val quantity: Long,
    val status: String,
    val reference: String? = null,
    val createdBy: String,
    val expiresAt: String,
    val createdAt: String,
    val updatedAt: String,
    val stock: StockLevelResponse? = null,
)

@Serializable
data class MovementResponse(
    val id: String,
    val type: String,
    val onHandDelta: Long,
    val reservedDelta: Long,
    val onHandAfter: Long,
    val reservedAfter: Long,
    val stockVersion: Long,
    val reason: String? = null,
    val reference: String? = null,
    val reservationId: String? = null,
    val actor: String,
    val occurredAt: String,
)

@Serializable
data class MovementsResponse(val items: List<MovementResponse>, val nextBefore: String? = null)

@Serializable
data class ActorResponse(
    val subject: String,
    val kind: String,
    val displayName: String,
    val clientId: String,
    val permissions: List<String>,
    val locations: List<String>,
)

@Serializable
data class DeadLetterResponse(
    val id: Long,
    val source: String,
    val status: String,
    val topic: String,
    val eventId: String? = null,
    val eventType: String? = null,
    val messageKey: String? = null,
    val sourcePartition: Int? = null,
    val sourceOffset: Long? = null,
    val attempts: Int,
    val error: String? = null,
    val payload: String,
    val createdAt: String,
    val updatedAt: String,
)

@Serializable
data class DeadLettersResponse(val items: List<DeadLetterResponse>, val nextAfterId: Long? = null)

@Serializable
data class ReplayResponse(val id: Long, val result: String, val detail: String? = null)

@Serializable
data class ErrorResponse(
    val code: String,
    val message: String,
    val details: Map<String, String> = emptyMap(),
    val traceId: String? = null,
)

@Serializable
data class HealthResponse(val status: String, val checks: Map<String, String>)

// ------------------------------------------------------------------ mappers

fun StockLevel.toResponse() = StockLevelResponse(
    sku = key.sku.value,
    locationId = key.locationId.value,
    onHand = onHand,
    reserved = reserved,
    available = available,
    oversold = oversold,
    version = version,
    updatedAt = updatedAt.toString(),
)

fun Reservation.toResponse(stock: StockLevel? = null) = ReservationResponse(
    id = id.toString(),
    sku = key.sku.value,
    locationId = key.locationId.value,
    quantity = quantity,
    status = status.name,
    reference = reference,
    createdBy = createdBy,
    expiresAt = expiresAt.toString(),
    createdAt = createdAt.toString(),
    updatedAt = updatedAt.toString(),
    stock = stock?.toResponse(),
)

fun StockMovement.toResponse() = MovementResponse(
    id = id.toString(),
    type = type.name,
    onHandDelta = onHandDelta,
    reservedDelta = reservedDelta,
    onHandAfter = onHandAfter,
    reservedAfter = reservedAfter,
    stockVersion = stockVersion,
    reason = reason,
    reference = reference,
    reservationId = reservationId?.toString(),
    actor = actor,
    occurredAt = occurredAt.toString(),
)

fun Actor.toResponse() = ActorResponse(
    subject = subject,
    kind = kind.name,
    displayName = displayName,
    clientId = clientId,
    permissions = permissions.map { it.claim }.sorted(),
    locations = when (val scope = locationScope) {
        LocationScope.All -> listOf("*")
        is LocationScope.Only -> scope.locations.map { it.value }.sorted()
    },
)

fun DeadLetter.toResponse() = DeadLetterResponse(
    id = id,
    source = source.name,
    status = status.name,
    topic = topic,
    eventId = eventId?.toString(),
    eventType = eventType,
    messageKey = messageKey,
    sourcePartition = sourcePartition,
    sourceOffset = sourceOffset,
    attempts = attempts,
    error = error,
    payload = payload,
    createdAt = createdAt.toString(),
    updatedAt = updatedAt.toString(),
)
