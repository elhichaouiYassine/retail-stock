package com.retail.stock.domain.model

import java.time.Instant
import java.util.*

enum class MovementType {
    ADJUSTMENT,
    SYNC,
    RESERVATION_CREATED,
    RESERVATION_RELEASED,
    RESERVATION_COMMITTED,
    RESERVATION_EXPIRED,
}

/** Immutable ledger line: the audit trail of every change applied to a [StockLevel]. */
data class StockMovement(
    val id: UUID,
    val key: StockKey,
    val type: MovementType,
    val onHandDelta: Long,
    val reservedDelta: Long,
    val onHandAfter: Long,
    val reservedAfter: Long,
    val stockVersion: Long,
    val reason: String?,
    val reference: String?,
    val reservationId: ReservationId?,
    val actor: String,
    val idempotencyKey: String?,
    val occurredAt: Instant,
)

/**
 * State-carrying integration event published to Kafka (`retail.stock.events.v1`).
 * Carries the full post-change position so consumers never need to call back,
 * and [stockVersion] so they can drop out-of-order or duplicated deliveries.
 */
data class StockEvent(
    val eventId: UUID,
    val type: MovementType,
    val key: StockKey,
    val stockVersion: Long,
    val onHandDelta: Long,
    val reservedDelta: Long,
    val onHand: Long,
    val reserved: Long,
    val available: Long,
    val reservationId: ReservationId?,
    val reason: String?,
    val reference: String?,
    val actor: String,
    val occurredAt: Instant,
) {
    companion object {
        fun of(movement: StockMovement): StockEvent = StockEvent(
            eventId = movement.id,
            type = movement.type,
            key = movement.key,
            stockVersion = movement.stockVersion,
            onHandDelta = movement.onHandDelta,
            reservedDelta = movement.reservedDelta,
            onHand = movement.onHandAfter,
            reserved = movement.reservedAfter,
            available = movement.onHandAfter - movement.reservedAfter,
            reservationId = movement.reservationId,
            reason = movement.reason,
            reference = movement.reference,
            actor = movement.actor,
            occurredAt = movement.occurredAt,
        )
    }
}
