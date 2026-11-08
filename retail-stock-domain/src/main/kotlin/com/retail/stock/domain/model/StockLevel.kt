package com.retail.stock.domain.model

import com.retail.stock.domain.error.InsufficientStock
import com.retail.stock.domain.error.ValidationFailed
import java.time.Instant

/**
 * Current inventory position of one SKU at one location.
 *
 * Invariants: `onHand >= 0`, `reserved >= 0`. `available` may go negative only after a physical
 * count ([syncTo]) reports fewer units than already promised to customers (an oversell that must be surfaced,
 * never hidden). Every state transition bumps [version], which is published on each event so consumers
 * can discard stale or duplicated messages.
 */
data class StockLevel(
    val key: StockKey,
    val onHand: Long,
    val reserved: Long,
    val version: Long,
    val updatedAt: Instant,
) {
    init {
        check(onHand >= 0) { "onHand must be >= 0 for $key (was $onHand)" }
        check(reserved >= 0) { "reserved must be >= 0 for $key (was $reserved)" }
    }

    val available: Long get() = onHand - reserved
    val oversold: Boolean get() = available < 0

    fun adjust(delta: Long, now: Instant): StockLevel {
        if (delta == 0L) throw ValidationFailed("Adjustment delta must not be zero")
        if (delta < 0 && available < -delta) {
            throw InsufficientStock(key.sku.value, key.locationId.value, -delta, available)
        }
        return copy(onHand = Math.addExact(onHand, delta), version = version + 1, updatedAt = now)
    }

    fun syncTo(countedOnHand: Long, now: Instant): StockLevel {
        if (countedOnHand < 0) throw ValidationFailed("Counted on-hand quantity must be >= 0")
        return copy(onHand = countedOnHand, version = version + 1, updatedAt = now)
    }

    fun reserve(quantity: Long, now: Instant): StockLevel {
        requirePositive(quantity)
        if (available < quantity) throw InsufficientStock(key.sku.value, key.locationId.value, quantity, available)
        return copy(reserved = Math.addExact(reserved, quantity), version = version + 1, updatedAt = now)
    }

    fun release(quantity: Long, now: Instant): StockLevel {
        requirePositive(quantity)
        check(reserved >= quantity) { "Cannot release $quantity from $key: only $reserved reserved" }
        return copy(reserved = reserved - quantity, version = version + 1, updatedAt = now)
    }

    /** Converts a reservation into a shipment/sale: units leave both reserved and on-hand. */
    fun commit(quantity: Long, now: Instant): StockLevel {
        requirePositive(quantity)
        check(reserved >= quantity) { "Cannot commit $quantity from $key: only $reserved reserved" }
        if (onHand < quantity) throw InsufficientStock(key.sku.value, key.locationId.value, quantity, onHand)
        return copy(
            onHand = onHand - quantity,
            reserved = reserved - quantity,
            version = version + 1,
            updatedAt = now,
        )
    }

    private fun requirePositive(quantity: Long) {
        if (quantity <= 0) throw ValidationFailed("Quantity must be > 0")
    }

    companion object {
        fun empty(key: StockKey, now: Instant): StockLevel =
            StockLevel(key = key, onHand = 0, reserved = 0, version = 0, updatedAt = now)
    }
}
