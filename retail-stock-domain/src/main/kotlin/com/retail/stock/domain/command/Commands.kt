package com.retail.stock.domain.command

import com.retail.stock.domain.model.IdempotencyKey
import com.retail.stock.domain.model.ReservationId
import com.retail.stock.domain.model.StockKey
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

/** Every mutation is idempotent: the fingerprint detects a reused key with a different payload. */
sealed interface StockCommand {
    val idempotencyKey: IdempotencyKey?

    fun fingerprint(): String = sha256(toString())
}

data class AdjustStock(
    val key: StockKey,
    val delta: Long,
    val reason: String,
    val reference: String?,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

data class AdjustmentLine(
    val key: StockKey,
    val delta: Long,
    val reason: String,
    val reference: String?,
)

data class BatchAdjustStock(
    val lines: List<AdjustmentLine>,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

data class SyncStock(
    val key: StockKey,
    val countedOnHand: Long,
    val source: String,
    val countedAt: Instant?,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

data class ReserveStock(
    val key: StockKey,
    val quantity: Long,
    val ttl: Duration?,
    val reference: String?,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

data class CommitReservation(
    val reservationId: ReservationId,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

data class ReleaseReservation(
    val reservationId: ReservationId,
    val reason: String?,
    override val idempotencyKey: IdempotencyKey?,
) : StockCommand

internal fun sha256(input: String): String {
    val digest = MessageDigest.getInstance("SHA-256").digest(input.toByteArray(Charsets.UTF_8))
    val hex = StringBuilder(digest.size * 2)
    for (byte in digest) {
        val v = byte.toInt() and 0xFF
        hex.append(HEX[v ushr 4]).append(HEX[v and 0x0F])
    }
    return hex.toString()
}

private val HEX = "0123456789abcdef".toCharArray()
