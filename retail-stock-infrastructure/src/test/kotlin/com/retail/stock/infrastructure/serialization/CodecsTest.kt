package com.retail.stock.infrastructure.serialization

import com.retail.stock.domain.model.*
import com.retail.stock.domain.port.OperationResult
import com.retail.stock.infrastructure.resilience.Backoff
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CodecsTest {
    private val now = Instant.parse("2026-11-04T10:15:30.123456Z")
    private val key = StockKey(Sku("SKU-42"), LocationId("STORE-001"))
    private val level = StockLevel(key, onHand = 12, reserved = 2, version = 7, updatedAt = now)

    @Test
    fun `stock event round trips through the Kafka wire format`() {
        val event = StockEvent(
            eventId = UuidV7.next(),
            type = MovementType.RESERVATION_CREATED,
            key = key,
            stockVersion = 7,
            onHandDelta = 0,
            reservedDelta = 2,
            onHand = 12,
            reserved = 2,
            available = 10,
            reservationId = ReservationId(UuidV7.next()),
            reason = null,
            reference = "ORDER-1",
            actor = "PARTNER:partner-acme",
            occurredAt = now,
        )
        val json = EventCodec.encode(event)
        assertTrue(json.contains("\"schemaVersion\":1"))
        assertEquals(event, EventCodec.decode(json))
    }

    @Test
    fun `idempotent results round trip`() {
        val reservation = Reservation(
            id = ReservationId(UuidV7.next()),
            key = key,
            quantity = 2,
            status = ReservationStatus.ACTIVE,
            reference = null,
            createdBy = "PARTNER:partner-acme",
            expiresAt = now.plusSeconds(900),
            createdAt = now,
            updatedAt = now,
        )
        val levels = OperationResult.Levels(listOf(level))
        val outcome = OperationResult.ReservationOutcome(reservation, level)
        assertEquals(levels, JsonOperationResultCodec.decode(JsonOperationResultCodec.encode(levels)))
        assertEquals(outcome, JsonOperationResultCodec.decode(JsonOperationResultCodec.encode(outcome)))
    }

    @Test
    fun `sync messages tolerate unknown fields`() {
        val message = EventCodec.decodeSync(
            """{"eventId":"0191f3c4-9c1e-7c3a-8f00-000000000001","sku":"SKU-42","locationId":"STORE-001","onHand":5,"source":"WMS","extra":true}""",
        )
        assertEquals(5, message.onHand)
        assertEquals(null, message.countedAt)
    }

    @Test
    fun `backoff grows exponentially and is capped`() {
        val base = Duration.ofMillis(100)
        val max = Duration.ofSeconds(5)
        val first = Backoff.delayFor(1, base, max)
        val tenth = Backoff.delayFor(10, base, max)
        assertTrue(first.toMillis() in 80..120, "first=$first")
        assertTrue(tenth <= max, "tenth=$tenth")
        assertTrue(Backoff.delayFor(100, base, max) <= max)
    }
}
