package com.retail.stock.domain.support

import com.retail.stock.domain.model.*
import com.retail.stock.domain.port.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * Serializable (single writer) in-memory adapter with snapshot rollback, mirroring the
 * transactional guarantees of the PostgreSQL adapter for fast, deterministic unit tests.
 */
class InMemoryStockStore : StockTransactions, StockQueries, OperationResultCodec {
    private val mutex = Mutex()

    private data class State(
        val levels: MutableMap<StockKey, StockLevel> = LinkedHashMap(),
        val reservations: MutableMap<ReservationId, Reservation> = LinkedHashMap(),
        val movements: MutableList<StockMovement> = ArrayList(),
        val outbox: MutableList<StockEvent> = ArrayList(),
        val idempotency: MutableMap<Pair<String, String>, Pair<String, String?>> = LinkedHashMap(),
    ) {
        fun deepCopy() = State(
            LinkedHashMap(levels),
            LinkedHashMap(reservations),
            ArrayList(movements),
            ArrayList(outbox),
            LinkedHashMap(idempotency),
        )
    }

    private var state = State()
    private val results = ConcurrentHashMap<String, OperationResult>()

    val outbox: List<StockEvent> get() = state.outbox.toList()
    val movements: List<StockMovement> get() = state.movements.toList()
    fun level(key: StockKey): StockLevel? = state.levels[key]
    fun reservation(id: ReservationId): Reservation? = state.reservations[id]

    fun seed(level: StockLevel) {
        state.levels[level.key] = level
    }

    override suspend fun <T> inTransaction(block: suspend StockTransaction.() -> T): T = mutex.withLock {
        val snapshot = state.deepCopy()
        try {
            Tx(state).block()
        } catch (e: Throwable) {
            state = snapshot
            throw e
        }
    }

    private inner class Tx(private val s: State) : StockTransaction {
        override suspend fun lockStockLevels(
            keys: Collection<StockKey>,
            createMissing: Boolean,
            now: Instant
        ): Map<StockKey, StockLevel> {
            val result = LinkedHashMap<StockKey, StockLevel>()
            for (key in keys.toSortedSet()) {
                val existing = s.levels[key]
                if (existing != null) {
                    result[key] = existing
                } else if (createMissing) {
                    val created = StockLevel.empty(key, now)
                    s.levels[key] = created
                    result[key] = created
                }
            }
            return result
        }

        override suspend fun saveStockLevels(levels: Collection<StockLevel>) {
            levels.forEach { s.levels[it.key] = it }
        }

        override suspend fun appendMovements(movements: List<StockMovement>) {
            s.movements += movements
        }

        override suspend fun enqueueEvents(events: List<StockEvent>) {
            s.outbox += events
        }

        override suspend fun insertReservation(reservation: Reservation) {
            check(s.reservations.putIfAbsent(reservation.id, reservation) == null) { "duplicate reservation" }
        }

        override suspend fun lockReservation(id: ReservationId): Reservation? = s.reservations[id]

        override suspend fun updateReservations(reservations: Collection<Reservation>) {
            reservations.forEach { s.reservations[it.id] = it }
        }

        override suspend fun lockExpiredReservations(now: Instant, limit: Int): List<Reservation> =
            s.reservations.values
                .filter { it.status == ReservationStatus.ACTIVE && !it.expiresAt.isAfter(now) }
                .sortedBy { it.expiresAt }
                .take(limit)

        override suspend fun claimIdempotencyKey(
            scope: String,
            key: IdempotencyKey,
            fingerprint: String
        ): IdempotencyClaim {
            val existing = s.idempotency[scope to key.value]
            return if (existing == null) {
                s.idempotency[scope to key.value] = fingerprint to null
                IdempotencyClaim.Claimed
            } else {
                IdempotencyClaim.Replay(existing.first, checkNotNull(existing.second) { "in-flight result" })
            }
        }

        override suspend fun storeIdempotentResult(scope: String, key: IdempotencyKey, payload: String) {
            val existing = checkNotNull(s.idempotency[scope to key.value])
            s.idempotency[scope to key.value] = existing.first to payload
        }
    }

    override suspend fun findLevel(key: StockKey): StockLevel? = state.levels[key]

    override suspend fun findLevelsBySku(sku: Sku, limit: Int): List<StockLevel> =
        state.levels.values.filter { it.key.sku == sku }.sortedBy { it.key }.take(limit)

    override suspend fun findReservation(id: ReservationId): Reservation? = state.reservations[id]

    override suspend fun findMovements(key: StockKey, limit: Int, before: Instant?): List<StockMovement> =
        state.movements.filter { it.key == key && (before == null || it.occurredAt.isBefore(before)) }
            .sortedByDescending { it.occurredAt }
            .take(limit)

    override fun encode(result: OperationResult): String {
        val token = UUID.randomUUID().toString()
        results[token] = result
        return token
    }

    override fun decode(payload: String): OperationResult = checkNotNull(results[payload]) { "unknown token" }
}
