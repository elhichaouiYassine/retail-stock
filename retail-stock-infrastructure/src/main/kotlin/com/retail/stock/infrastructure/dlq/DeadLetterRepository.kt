package com.retail.stock.infrastructure.dlq

import com.retail.stock.infrastructure.db.*
import java.time.Instant
import java.util.*

enum class DeadLetterSource { OUTBOX, CONSUMER }

enum class DeadLetterStatus { PENDING, REPLAYED, DISCARDED }

data class DeadLetter(
    val id: Long,
    val source: DeadLetterSource,
    val eventId: UUID?,
    val eventType: String?,
    val topic: String,
    val messageKey: String?,
    val payload: String,
    val sourcePartition: Int?,
    val sourceOffset: Long?,
    val attempts: Int,
    val error: String?,
    val status: DeadLetterStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class NewDeadLetter(
    val source: DeadLetterSource,
    val topic: String,
    val payload: String,
    val messageKey: String?,
    val eventId: UUID? = null,
    val eventType: String? = null,
    val sourcePartition: Int? = null,
    val sourceOffset: Long? = null,
    val attempts: Int,
    val error: String,
)

class DeadLetterRepository(private val database: Database) {

    suspend fun insert(letter: NewDeadLetter): Long = database.transaction { c ->
        c.prepared(
            """
            INSERT INTO dead_letters (source, event_id, event_type, topic, message_key, payload, source_partition, source_offset, attempts, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """.trimIndent(),
        ) { ps ->
            ps.setString(1, letter.source.name)
            ps.setNullableUuid(2, letter.eventId)
            ps.setNullableString(3, letter.eventType)
            ps.setString(4, letter.topic)
            ps.setNullableString(5, letter.messageKey)
            ps.setString(6, letter.payload)
            ps.setNullableInt(7, letter.sourcePartition)
            ps.setNullableLong(8, letter.sourceOffset)
            ps.setInt(9, letter.attempts)
            ps.setString(10, letter.error.take(2_000))
            ps.query { it.getLong("id") }.single()
        }
    }

    suspend fun list(status: DeadLetterStatus, limit: Int, afterId: Long?): List<DeadLetter> =
        database.transaction(readOnly = true) { c ->
            c.prepared("$SELECT WHERE status = ? AND id > ? ORDER BY id LIMIT ?") { ps ->
                ps.setString(1, status.name)
                ps.setLong(2, afterId ?: 0L)
                ps.setInt(3, limit)
                ps.query(::map)
            }
        }

    suspend fun find(id: Long): DeadLetter? = database.transaction(readOnly = true) { c ->
        c.prepared("$SELECT WHERE id = ?") { ps ->
            ps.setLong(1, id)
            ps.query(::map).firstOrNull()
        }
    }

    suspend fun countPending(): Long = database.transaction(readOnly = true) { c ->
        c.prepared("SELECT count(*) FROM dead_letters WHERE status = 'PENDING'") { ps ->
            ps.query { it.getLong(1) }.single()
        }
    }

    /** Moves an OUTBOX dead letter back into the outbox (same event id: consumers dedupe). */
    suspend fun requeueToOutbox(id: Long): Boolean = database.transaction { c ->
        c.prepared(
            """
            WITH dl AS (
                UPDATE dead_letters SET status = 'REPLAYED', updated_at = now()
                WHERE id = ? AND status = 'PENDING' AND source = 'OUTBOX' AND event_id IS NOT NULL
                RETURNING event_id, event_type, topic, message_key, payload
            )
            INSERT INTO event_outbox (event_id, event_type, topic, message_key, payload)
            SELECT event_id, coalesce(event_type, 'UNKNOWN'), topic, coalesce(message_key, ''), payload FROM dl
            ON CONFLICT (event_id) DO NOTHING
            """.trimIndent(),
        ) { ps ->
            ps.setLong(1, id)
            ps.executeUpdate()
        }
        // The UPDATE inside the CTE decides success, the INSERT may be a no-op if the event is still queued.
        c.prepared("SELECT status FROM dead_letters WHERE id = ?") { ps ->
            ps.setLong(1, id)
            ps.query { it.getString(1) }.firstOrNull() == DeadLetterStatus.REPLAYED.name
        }
    }

    suspend fun markStatus(id: Long, from: DeadLetterStatus, to: DeadLetterStatus): Boolean =
        database.transaction { c ->
            c.prepared("UPDATE dead_letters SET status = ?, updated_at = now() WHERE id = ? AND status = ?") { ps ->
                ps.setString(1, to.name)
                ps.setLong(2, id)
                ps.setString(3, from.name)
                ps.executeUpdate() == 1
            }
        }

    private companion object {
        const val SELECT =
            """SELECT id, source, event_id, event_type, topic, message_key, payload, source_partition, source_offset,
            attempts, error, status, created_at, updated_at FROM dead_letters"""

        fun map(rs: java.sql.ResultSet) = DeadLetter(
            id = rs.getLong("id"),
            source = DeadLetterSource.valueOf(rs.getString("source")),
            eventId = rs.nullableUuid("event_id"),
            eventType = rs.getString("event_type"),
            topic = rs.getString("topic"),
            messageKey = rs.getString("message_key"),
            payload = rs.getString("payload"),
            sourcePartition = rs.nullableInt("source_partition"),
            sourceOffset = rs.nullableLong("source_offset"),
            attempts = rs.getInt("attempts"),
            error = rs.getString("error"),
            status = DeadLetterStatus.valueOf(rs.getString("status")),
            createdAt = rs.instant("created_at"),
            updatedAt = rs.instant("updated_at"),
        )
    }
}
