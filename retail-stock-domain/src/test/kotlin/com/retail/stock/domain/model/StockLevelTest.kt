package com.retail.stock.domain.model

import com.retail.stock.domain.error.InsufficientStock
import com.retail.stock.domain.error.ValidationFailed
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StockLevelTest {
    private val now = Instant.parse("2026-11-04T10:00:00Z")
    private val key = StockKey(Sku("SKU-1"), LocationId("STORE-001"))

    @Test
    fun `transitions bump version and keep invariants`() {
        val level = StockLevel.empty(key, now).adjust(10, now).reserve(4, now).commit(3, now).release(1, now)
        assertEquals(7, level.onHand)
        assertEquals(0, level.reserved)
        assertEquals(4, level.version)
    }

    @Test
    fun `rejects invalid quantities`() {
        val level = StockLevel.empty(key, now).adjust(5, now)
        assertFailsWith<ValidationFailed> { level.adjust(0, now) }
        assertFailsWith<ValidationFailed> { level.reserve(0, now) }
        assertFailsWith<InsufficientStock> { level.reserve(6, now) }
        assertFailsWith<IllegalStateException> { level.release(1, now) }
    }

    @Test
    fun `identifiers are validated`() {
        assertFailsWith<ValidationFailed> { Sku("lower-case") }
        assertFailsWith<ValidationFailed> { LocationId("") }
        assertFailsWith<ValidationFailed> { IdempotencyKey("short") }
        assertEquals("SKU-1|STORE-001", key.messageKey())
    }

    @Test
    fun `uuid v7 is time ordered and well formed`() {
        val a = UuidV7.next(1_000)
        val b = UuidV7.next(2_000)
        assertEquals(7, a.version())
        assertEquals(2, a.variant())
        assertTrue(a.toString() < b.toString())
    }
}
