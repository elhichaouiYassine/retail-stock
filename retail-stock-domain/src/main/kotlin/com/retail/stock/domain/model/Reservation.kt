package com.retail.stock.domain.model

import com.retail.stock.domain.error.ReservationNotActive
import java.time.Instant

enum class ReservationStatus { ACTIVE, COMMITTED, RELEASED, EXPIRED }

data class Reservation(
    val id: ReservationId,
    val key: StockKey,
    val quantity: Long,
    val status: ReservationStatus,
    val reference: String?,
    val createdBy: String,
    val expiresAt: Instant,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    fun isExpiredAt(now: Instant): Boolean = !expiresAt.isAfter(now)

    fun transitionTo(target: ReservationStatus, now: Instant): Reservation {
        if (status != ReservationStatus.ACTIVE) throw ReservationNotActive(id.toString(), status.name)
        require(target != ReservationStatus.ACTIVE) { "Cannot transition reservation back to ACTIVE" }
        return copy(status = target, updatedAt = now)
    }
}
