package com.retail.stock.infrastructure.db

import com.retail.stock.domain.model.*
import com.retail.stock.domain.port.IdempotencyClaim
import com.retail.stock.domain.port.StockQueries
import com.retail.stock.domain.port.StockTransaction
import com.retail.stock.domain.port.StockTransactions
import com.retail.stock.infrastructure.outbox.OutboxSignal
import com.retail.stock.infrastructure.serialization.EventCodec
import java.sql.Connection
import java.sql.ResultSet
import java.time.Instant

/**
 * PostgreSQL adapter for the stock aggregate.
 *
 * Concurrency model: pessimistic row locks (`FOR NO KEY UPDATE`, compatible with FK checks) taken in
 * byte-wise (`COLLATE "C"`) key order, which matches [StockKey.compareTo] for the ASCII identifier alphabet.
 * Hot SKUs therefore serialize per row, never deadlock, and wait at most `lock_timeout`.
 */
class PostgresStockStore(
    private val database: Database,
    private val eventsTopic: String,
    private val outboxSignal: OutboxSignal,
) : StockTransactions, StockQueries {

    override suspend fun <T> inTransaction(block: suspend StockTransaction.() -> T): T {
        var enqueued = false
        val result = database.transaction { connection ->
            val tx = PgTransaction(connection, eventsTopic)
            val value = tx.block()
            enqueued = tx.enqueuedEvents
            value
        }
        // Wake the local relay right after commit: sub-poll-interval publish latency on the happy path.
        if (enqueued) outboxSignal.notifyPending()
        return result
    }

    // ---------------------------------------------------------------- read side

    override suspend fun findLevel(key: StockKey): StockLevel? = database.transaction(readOnly = true) { c ->
        c.prepared("$SELECT_LEVEL WHERE sku = ? AND location_id = ?") { ps ->
            ps.setString(1, key.sku.value)
            ps.setString(2, key.locationId.value)
            ps.query(::mapLevel).firstOrNull()
        }
    }

    override suspend fun findLevelsBySku(sku: Sku, limit: Int): List<StockLevel> =
        database.transaction(readOnly = true) { c ->
            c.prepared("$SELECT_LEVEL WHERE sku = ? ORDER BY location_id LIMIT ?") { ps ->
                ps.setString(1, sku.value)
                ps.setInt(2, limit)
                ps.query(::mapLevel)
            }
        }

    override suspend fun findReservation(id: ReservationId): Reservation? = database.transaction(readOnly = true) { c ->
        c.prepared("$SELECT_RESERVATION WHERE id = ?") { ps ->
            ps.setObject(1, id.value)
            ps.query(::mapReservation).firstOrNull()
        }
    }

    override suspend fun findMovements(key: StockKey, limit: Int, before: Instant?): List<StockMovement> =
        database.transaction(readOnly = true) { c ->
            c.prepared(
                """
                SELECT id, sku, location_id, movement_type, on_hand_delta, reserved_delta, on_hand_after, reserved_after,
                       stock_version, reason, reference, reservation_id, actor, idempotency_key, occurred_at
                FROM stock_movements
                WHERE sku = ? AND location_id = ? AND occurred_at < ?
                ORDER BY occurred_at DESC
                LIMIT ?
                """.trimIndent(),
            ) { ps ->
                ps.setString(1, key.sku.value)
                ps.setString(2, key.locationId.value)
                ps.setInstant(3, before ?: Instant.now().plusSeconds(60))
                ps.setInt(4, limit)
                ps.query(::mapMovement)
            }
        }

    // ---------------------------------------------------------------- write side

    private class PgTransaction(private val c: Connection, private val eventsTopic: String) : StockTransaction {
        var enqueuedEvents = false
            private set

        override suspend fun lockStockLevels(
            keys: Collection<StockKey>,
            createMissing: Boolean,
            now: Instant
        ): Map<StockKey, StockLevel> {
            if (keys.isEmpty()) return emptyMap()
            val sorted = keys.toSortedSet()
            val skus = c.textArray(sorted.map { it.sku.value })
            val locations = c.textArray(sorted.map { it.locationId.value })
            if (createMissing) {
                c.prepared(
                    """
                    INSERT INTO stock_levels (sku, location_id, on_hand, reserved, version, updated_at)
                    SELECT k.sku, k.location_id, 0, 0, 0, ?
                    FROM unnest(?::text[], ?::text[]) AS k(sku, location_id)
                    ORDER BY k.sku COLLATE "C", k.location_id COLLATE "C"
                    ON CONFLICT (sku, location_id) DO NOTHING
                    """.trimIndent(),
                ) { ps ->
                    ps.setInstant(1, now)
                    ps.setArray(2, skus)
                    ps.setArray(3, locations)
                    ps.executeUpdate()
                }
            }
            return c.prepared(
                """
                SELECT s.sku, s.location_id, s.on_hand, s.reserved, s.version, s.updated_at
                FROM stock_levels s
                JOIN unnest(?::text[], ?::text[]) AS k(sku, location_id)
                  ON s.sku = k.sku AND s.location_id = k.location_id
                ORDER BY s.sku COLLATE "C", s.location_id COLLATE "C"
                FOR NO KEY UPDATE OF s
                """.trimIndent(),
            ) { ps ->
                ps.setArray(1, skus)
                ps.setArray(2, locations)
                ps.query(::mapLevel).associateBy { it.key }
            }
        }

        override suspend fun saveStockLevels(levels: Collection<StockLevel>) {
            c.prepared(
                "UPDATE stock_levels SET on_hand = ?, reserved = ?, version = ?, updated_at = ? WHERE sku = ? AND location_id = ?",
            ) { ps ->
                for (level in levels) {
                    ps.setLong(1, level.onHand)
                    ps.setLong(2, level.reserved)
                    ps.setLong(3, level.version)
                    ps.setInstant(4, level.updatedAt)
                    ps.setString(5, level.key.sku.value)
                    ps.setString(6, level.key.locationId.value)
                    ps.addBatch()
                }
                val counts = ps.executeBatch()
                check(counts.all { it == 1 || it == java.sql.Statement.SUCCESS_NO_INFO }) { "Lost update on stock_levels" }
            }
        }

        override suspend fun appendMovements(movements: List<StockMovement>) {
            c.prepared(
                """
                INSERT INTO stock_movements (id, sku, location_id, movement_type, on_hand_delta, reserved_delta, on_hand_after,
                    reserved_after, stock_version, reason, reference, reservation_id, actor, idempotency_key, occurred_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
            ) { ps ->
                for (m in movements) {
                    ps.setObject(1, m.id)
                    ps.setString(2, m.key.sku.value)
                    ps.setString(3, m.key.locationId.value)
                    ps.setString(4, m.type.name)
                    ps.setLong(5, m.onHandDelta)
                    ps.setLong(6, m.reservedDelta)
                    ps.setLong(7, m.onHandAfter)
                    ps.setLong(8, m.reservedAfter)
                    ps.setLong(9, m.stockVersion)
                    ps.setNullableString(10, m.reason)
                    ps.setNullableString(11, m.reference)
                    ps.setNullableUuid(12, m.reservationId?.value)
                    ps.setString(13, m.actor)
                    ps.setNullableString(14, m.idempotencyKey)
                    ps.setInstant(15, m.occurredAt)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }

        override suspend fun enqueueEvents(events: List<StockEvent>) {
            if (events.isEmpty()) return
            c.prepared(
                "INSERT INTO event_outbox (event_id, event_type, topic, message_key, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            ) { ps ->
                for (event in events) {
                    ps.setObject(1, event.eventId)
                    ps.setString(2, event.type.name)
                    ps.setString(3, eventsTopic)
                    ps.setString(4, event.key.messageKey())
                    ps.setString(5, EventCodec.encode(event))
                    ps.setInstant(6, event.occurredAt)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            enqueuedEvents = true
        }

        override suspend fun insertReservation(reservation: Reservation) {
            c.prepared(
                """
                INSERT INTO reservations (id, sku, location_id, quantity, status, reference, created_by, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
            ) { ps ->
                ps.setObject(1, reservation.id.value)
                ps.setString(2, reservation.key.sku.value)
                ps.setString(3, reservation.key.locationId.value)
                ps.setLong(4, reservation.quantity)
                ps.setString(5, reservation.status.name)
                ps.setNullableString(6, reservation.reference)
                ps.setString(7, reservation.createdBy)
                ps.setInstant(8, reservation.expiresAt)
                ps.setInstant(9, reservation.createdAt)
                ps.setInstant(10, reservation.updatedAt)
                ps.executeUpdate()
            }
        }

        override suspend fun lockReservation(id: ReservationId): Reservation? =
            c.prepared("$SELECT_RESERVATION WHERE id = ? FOR NO KEY UPDATE") { ps ->
                ps.setObject(1, id.value)
                ps.query(::mapReservation).firstOrNull()
            }

        override suspend fun updateReservations(reservations: Collection<Reservation>) {
            if (reservations.isEmpty()) return
            c.prepared("UPDATE reservations SET status = ?, updated_at = ? WHERE id = ?") { ps ->
                for (r in reservations) {
                    ps.setString(1, r.status.name)
                    ps.setInstant(2, r.updatedAt)
                    ps.setObject(3, r.id.value)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }

        override suspend fun lockExpiredReservations(now: Instant, limit: Int): List<Reservation> =
            c.prepared(
                "$SELECT_RESERVATION WHERE status = 'ACTIVE' AND expires_at <= ? ORDER BY expires_at LIMIT ? FOR NO KEY UPDATE SKIP LOCKED",
            ) { ps ->
                ps.setInstant(1, now)
                ps.setInt(2, limit)
                ps.query(::mapReservation)
            }

        override suspend fun claimIdempotencyKey(
            scope: String,
            key: IdempotencyKey,
            fingerprint: String
        ): IdempotencyClaim {
            val inserted = c.prepared(
                "INSERT INTO idempotency_keys (scope, idempotency_key, fingerprint) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            ) { ps ->
                ps.setString(1, scope)
                ps.setString(2, key.value)
                ps.setString(3, fingerprint)
                ps.executeUpdate()
            }
            if (inserted == 1) return IdempotencyClaim.Claimed

            return c.prepared("SELECT fingerprint, response FROM idempotency_keys WHERE scope = ? AND idempotency_key = ?") { ps ->
                ps.setString(1, scope)
                ps.setString(2, key.value)
                ps.query { rs -> rs.getString("fingerprint") to rs.getString("response") }.single()
            }.let { (storedFingerprint, response) ->
                IdempotencyClaim.Replay(
                    storedFingerprint,
                    checkNotNull(response) { "Idempotency key ${key.value} has no stored result" })
            }
        }

        override suspend fun storeIdempotentResult(scope: String, key: IdempotencyKey, payload: String) {
            c.prepared("UPDATE idempotency_keys SET response = ? WHERE scope = ? AND idempotency_key = ?") { ps ->
                ps.setString(1, payload)
                ps.setString(2, scope)
                ps.setString(3, key.value)
                check(ps.executeUpdate() == 1) { "Idempotency key ${key.value} vanished" }
            }
        }
    }

    private companion object {
        const val SELECT_LEVEL = "SELECT sku, location_id, on_hand, reserved, version, updated_at FROM stock_levels"
        const val SELECT_RESERVATION =
            "SELECT id, sku, location_id, quantity, status, reference, created_by, expires_at, created_at, updated_at FROM reservations"

        fun mapLevel(rs: ResultSet) = StockLevel(
            key = StockKey(Sku(rs.getString("sku")), LocationId(rs.getString("location_id"))),
            onHand = rs.getLong("on_hand"),
            reserved = rs.getLong("reserved"),
            version = rs.getLong("version"),
            updatedAt = rs.instant("updated_at"),
        )

        fun mapReservation(rs: ResultSet) = Reservation(
            id = ReservationId(rs.uuid("id")),
            key = StockKey(Sku(rs.getString("sku")), LocationId(rs.getString("location_id"))),
            quantity = rs.getLong("quantity"),
            status = ReservationStatus.valueOf(rs.getString("status")),
            reference = rs.getString("reference"),
            createdBy = rs.getString("created_by"),
            expiresAt = rs.instant("expires_at"),
            createdAt = rs.instant("created_at"),
            updatedAt = rs.instant("updated_at"),
        )

        fun mapMovement(rs: ResultSet) = StockMovement(
            id = rs.uuid("id"),
            key = StockKey(Sku(rs.getString("sku")), LocationId(rs.getString("location_id"))),
            type = MovementType.valueOf(rs.getString("movement_type")),
            onHandDelta = rs.getLong("on_hand_delta"),
            reservedDelta = rs.getLong("reserved_delta"),
            onHandAfter = rs.getLong("on_hand_after"),
            reservedAfter = rs.getLong("reserved_after"),
            stockVersion = rs.getLong("stock_version"),
            reason = rs.getString("reason"),
            reference = rs.getString("reference"),
            reservationId = rs.nullableUuid("reservation_id")?.let(::ReservationId),
            actor = rs.getString("actor"),
            idempotencyKey = rs.getString("idempotency_key"),
            occurredAt = rs.instant("occurred_at"),
        )
    }
}
