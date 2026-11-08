package com.retail.stock.infrastructure.db

import java.time.Duration

/** Housekeeping that keeps hot tables small at 6M movements/day. All deletes are batched to bound lock time. */
class MaintenanceRepository(private val database: Database) {

    suspend fun ensureMovementPartitions(monthsAhead: Int): Int = database.transaction { c ->
        c.prepared("SELECT ensure_stock_movement_partitions(?)") { ps ->
            ps.setInt(1, monthsAhead)
            ps.query { it.getInt(1) }.single()
        }
    }

    suspend fun purgeIdempotencyKeys(olderThan: Duration, batchSize: Int = 5_000): Int =
        purgeInBatches(
            """
            DELETE FROM idempotency_keys
            WHERE ctid = ANY(ARRAY(
                SELECT ctid FROM idempotency_keys WHERE created_at < now() - make_interval(secs => ?) LIMIT ?
            ))
            """.trimIndent(),
            olderThan,
            batchSize,
        )

    suspend fun purgeClosedReservations(olderThan: Duration, batchSize: Int = 5_000): Int =
        purgeInBatches(
            """
            DELETE FROM reservations
            WHERE id IN (
                SELECT id FROM reservations
                WHERE status <> 'ACTIVE' AND updated_at < now() - make_interval(secs => ?)
                LIMIT ?
            )
            """.trimIndent(),
            olderThan,
            batchSize,
        )

    private suspend fun purgeInBatches(sql: String, olderThan: Duration, batchSize: Int): Int {
        var total = 0
        while (true) {
            val deleted = database.transaction { c ->
                c.prepared(sql) { ps ->
                    ps.setDouble(1, olderThan.seconds.toDouble())
                    ps.setInt(2, batchSize)
                    ps.executeUpdate()
                }
            }
            total += deleted
            if (deleted < batchSize) return total
        }
    }
}
