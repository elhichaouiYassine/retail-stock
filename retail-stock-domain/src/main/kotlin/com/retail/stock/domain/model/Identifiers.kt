package com.retail.stock.domain.model

import com.retail.stock.domain.error.ValidationFailed
import java.util.*
import java.util.concurrent.ThreadLocalRandom

private val SKU_PATTERN = Regex("^[A-Z0-9][A-Z0-9._-]{0,63}$")
private val LOCATION_PATTERN = Regex("^[A-Z0-9][A-Z0-9_-]{0,31}$")
private val IDEMPOTENCY_KEY_PATTERN = Regex("^[A-Za-z0-9._:-]{8,128}$")

/** Stock keeping unit. Upper-case alphanumerics plus `.`, `_`, `-` (max 64 chars). */
@JvmInline
value class Sku(val value: String) {
    init {
        if (!SKU_PATTERN.matches(value)) throw ValidationFailed("Invalid SKU '$value'")
    }

    override fun toString(): String = value
}

/** Store, warehouse or partner node identifier (max 32 chars). */
@JvmInline
value class LocationId(val value: String) {
    init {
        if (!LOCATION_PATTERN.matches(value)) throw ValidationFailed("Invalid location id '$value'")
    }

    override fun toString(): String = value
}

/** Composite identity of a stock row; the natural lock ordering key. */
data class StockKey(val sku: Sku, val locationId: LocationId) : Comparable<StockKey> {
    override fun compareTo(other: StockKey): Int {
        val bySku = sku.value.compareTo(other.sku.value)
        return if (bySku != 0) bySku else locationId.value.compareTo(other.locationId.value)
    }

    /** Kafka partitioning key: guarantees per-stock-item ordering on a single partition. */
    fun messageKey(): String = "${sku.value}|${locationId.value}"

    override fun toString(): String = messageKey()
}

@JvmInline
value class ReservationId(val value: UUID) {
    override fun toString(): String = value.toString()

    companion object {
        fun parse(raw: String): ReservationId =
            try {
                ReservationId(UUID.fromString(raw))
            } catch (e: IllegalArgumentException) {
                throw ValidationFailed("Invalid reservation id '$raw'")
            }
    }
}

/** Client supplied key that makes a mutation safe to retry (HTTP `Idempotency-Key` header, Kafka event id). */
@JvmInline
value class IdempotencyKey(val value: String) {
    init {
        if (!IDEMPOTENCY_KEY_PATTERN.matches(value)) {
            throw ValidationFailed("Idempotency key must match ${IDEMPOTENCY_KEY_PATTERN.pattern}")
        }
    }

    override fun toString(): String = value
}

/**
 * RFC 9562 UUIDv7 generator: time ordered ids keep B-tree inserts append-only,
 * which matters at 6M movements/day.
 */
object UuidV7 {
    fun next(epochMillis: Long = System.currentTimeMillis()): UUID {
        val random = ThreadLocalRandom.current()
        val timestamp = epochMillis and 0xFFFF_FFFF_FFFFL
        val randA = (random.nextInt() and 0xFFF).toLong()
        val msb = (timestamp shl 16) or (0x7L shl 12) or randA
        val lsb = (random.nextLong() and 0x3FFF_FFFF_FFFF_FFFFL) or Long.MIN_VALUE
        return UUID(msb, lsb)
    }
}
