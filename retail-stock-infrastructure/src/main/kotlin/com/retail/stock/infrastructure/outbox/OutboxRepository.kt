package com.retail.stock.infrastructure.outbox

import com.retail.stock.infrastructure.db.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Duration
import java.time.Instant
import java.util.*

data class OutboxRecord(
    val id: Long,
    val eventId: UUID,
    val eventType: String,
    val topic: String,
    val messageKey: String,
    val payload: String,
    val attempts: Int,
    val createdAt: Instant,
)

data class OutboxFailure(val record: OutboxRecord, val error: String, val retryIn: Duration)

data class OutboxBacklog(val pending: Long, val oldestAgeSeconds: Double)

/** Conflated in-process wake-up: committed writers poke the relay instead of waiting for the next poll tick. */
class OutboxSignal {
    private val channel = Channel<Unit>(Channel.CONFLATED)

    fun notifyPending() {
        channel.trySend(Unit)
    }

    suspend fun awaitPending(timeout: Duration) {
        withTimeoutOrNull(timeout.toMillis()) { channel.receive() }
    }
}

class OutboxRepository(private val database: Database) {

    /**
     * Leases up to [limit] due events to [owner]. `SKIP LOCKED` lets every pod drain the outbox
     * concurrently without blocking each other; an expired lease (crashed pod) makes rows claimable again.
     */
    suspend fun claim(limit: Int, lease: Duration, owner: String): List<OutboxRecord> = database.transaction { c ->
        c.prepared(
            """
            WITH candidates AS (
                SELECT id FROM event_outbox
                WHERE next_attempt_at <= now() AND (locked_until IS NULL OR locked_until < now())
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE event_outbox o
            SET locked_until = now() + make_interval(secs => ?), locked_by = ?
            FROM candidates c
            WHERE o.id = c.id
            RETURNING o.id, o.event_id, o.event_type, o.topic, o.message_key, o.payload, o.attempts, o.created_at
            """.trimIndent(),
        ) { ps ->
            ps.setInt(1, limit)
            ps.setDouble(2, lease.toMillis() / 1000.0)
            ps.setString(3, owner)
            ps.query { rs ->
                OutboxRecord(
                    id = rs.getLong("id"),
                    eventId = rs.uuid("event_id"),
                    eventType = rs.getString("event_type"),
                    topic = rs.getString("topic"),
                    messageKey = rs.getString("message_key"),
                    payload = rs.getString("payload"),
                    attempts = rs.getInt("attempts"),
                    createdAt = rs.instant("created_at"),
                )
            }.sortedBy { it.id }
        }
    }

    suspend fun markPublished(ids: Collection<Long>, owner: String): Int {
        if (ids.isEmpty()) return 0
        return database.transaction { c ->
            c.prepared("DELETE FROM event_outbox WHERE id = ANY(?) AND locked_by = ?") { ps ->
                ps.setArray(1, c.bigintArray(ids))
                ps.setString(2, owner)
                ps.executeUpdate()
            }
        }
    }

    suspend fun reschedule(failures: Collection<OutboxFailure>, owner: String) {
        if (failures.isEmpty()) return
        database.transaction { c ->
            c.prepared(
                """
                UPDATE event_outbox
                SET attempts = attempts + 1,
                    next_attempt_at = now() + make_interval(secs => ?),
                    locked_until = NULL,
                    locked_by = NULL,
                    last_error = ?
                WHERE id = ? AND locked_by = ?
                """.trimIndent(),
            ) { ps ->
                for (f in failures) {
                    ps.setDouble(1, f.retryIn.toMillis() / 1000.0)
                    ps.setString(2, f.error.take(MAX_ERROR_LENGTH))
                    ps.setLong(3, f.record.id)
                    ps.setString(4, owner)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    /** Atomically moves poisoned / exhausted events into the PostgreSQL DLQ. */
    suspend fun deadLetter(failures: Collection<OutboxFailure>, owner: String): Int {
        if (failures.isEmpty()) return 0
        return database.transaction { c ->
            c.prepared(
                """
                WITH moved AS (
                    DELETE FROM event_outbox WHERE id = ? AND locked_by = ?
                    RETURNING event_id, event_type, topic, message_key, payload, attempts
                )
                INSERT INTO dead_letters (source, event_id, event_type, topic, message_key, payload, attempts, error)
                SELECT 'OUTBOX', event_id, event_type, topic, message_key, payload, attempts + 1, ? FROM moved
                """.trimIndent(),
            ) { ps ->
                for (f in failures) {
                    ps.setLong(1, f.record.id)
                    ps.setString(2, owner)
                    ps.setString(3, f.error.take(MAX_ERROR_LENGTH))
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
    }

    /** Cheap backlog probe: count is capped so a multi-million row backlog (broker outage) stays O(cap). */
    suspend fun backlog(cap: Int = 1_000_000): OutboxBacklog = database.transaction(readOnly = true) { c ->
        val pending = c.prepared("SELECT count(*) FROM (SELECT 1 FROM event_outbox LIMIT ?) t") { ps ->
            ps.setInt(1, cap)
            ps.query { it.getLong(1) }.single()
        }
        val oldest = c.prepared(
            "SELECT EXTRACT(EPOCH FROM (now() - created_at))::float8 FROM event_outbox ORDER BY id LIMIT 1",
        ) { ps -> ps.query { it.getDouble(1) }.firstOrNull() ?: 0.0 }
        OutboxBacklog(pending, oldest)
    }

    private companion object {
        const val MAX_ERROR_LENGTH = 2_000
    }
}
