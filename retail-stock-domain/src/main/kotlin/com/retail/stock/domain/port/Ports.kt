package com.retail.stock.domain.port

import com.retail.stock.domain.model.*
import java.time.Instant

/**
 * Unit of work. Implementations MUST run [block] inside a single ACID transaction and
 * MUST retry transparently on serialization failures / deadlocks (the block is side-effect free
 * outside the transaction).
 */
interface StockTransactions {
    suspend fun <T> inTransaction(block: suspend StockTransaction.() -> T): T
}

interface StockTransaction {
    /**
     * Locks (`SELECT ... FOR UPDATE`) the rows for [keys] in deterministic [StockKey] order to make
     * deadlocks between concurrent multi-row transactions impossible. When [createMissing] is true,
     * absent rows are inserted with zero quantities first.
     */
    suspend fun lockStockLevels(
        keys: Collection<StockKey>,
        createMissing: Boolean,
        now: Instant
    ): Map<StockKey, StockLevel>

    suspend fun saveStockLevels(levels: Collection<StockLevel>)

    suspend fun appendMovements(movements: List<StockMovement>)

    /** Transactional outbox: events become visible to the Kafka relay only if the transaction commits. */
    suspend fun enqueueEvents(events: List<StockEvent>)

    suspend fun insertReservation(reservation: Reservation)

    suspend fun lockReservation(id: ReservationId): Reservation?

    suspend fun updateReservations(reservations: Collection<Reservation>)

    /** Locks up to [limit] expired ACTIVE reservations, skipping rows locked by other workers. */
    suspend fun lockExpiredReservations(now: Instant, limit: Int): List<Reservation>

    /**
     * Claims an idempotency key inside the current transaction. Concurrent claims of the same key block
     * on the unique index until the first transaction finishes, then observe its stored result.
     */
    suspend fun claimIdempotencyKey(scope: String, key: IdempotencyKey, fingerprint: String): IdempotencyClaim

    suspend fun storeIdempotentResult(scope: String, key: IdempotencyKey, payload: String)
}

sealed interface IdempotencyClaim {
    data object Claimed : IdempotencyClaim
    data class Replay(val fingerprint: String, val payload: String) : IdempotencyClaim
}

/** Read side; may be served from a replica. */
interface StockQueries {
    suspend fun findLevel(key: StockKey): StockLevel?
    suspend fun findLevelsBySku(sku: Sku, limit: Int): List<StockLevel>
    suspend fun findReservation(id: ReservationId): Reservation?
    suspend fun findMovements(key: StockKey, limit: Int, before: Instant?): List<StockMovement>
}

/** Result shapes persisted for idempotent replay. */
sealed interface OperationResult {
    data class Levels(val levels: List<StockLevel>) : OperationResult
    data class ReservationOutcome(val reservation: Reservation, val level: StockLevel) : OperationResult
}

interface OperationResultCodec {
    fun encode(result: OperationResult): String
    fun decode(payload: String): OperationResult
}
