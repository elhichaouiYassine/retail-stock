package com.retail.stock.domain.service

import com.retail.stock.domain.command.*
import com.retail.stock.domain.error.*
import com.retail.stock.domain.model.IdempotencyKey
import com.retail.stock.domain.model.MovementType
import com.retail.stock.domain.model.ReservationStatus
import com.retail.stock.domain.model.StockLevel
import com.retail.stock.domain.security.Actor
import com.retail.stock.domain.support.Fixtures.KEY_A1
import com.retail.stock.domain.support.Fixtures.KEY_A2
import com.retail.stock.domain.support.Fixtures.KEY_B1
import com.retail.stock.domain.support.Fixtures.opsTeammate
import com.retail.stock.domain.support.Fixtures.partnerStore1
import com.retail.stock.domain.support.InMemoryStockStore
import com.retail.stock.domain.support.MutableClock
import com.retail.stock.domain.support.asActor
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StockServiceTest {
    private val clock = MutableClock()
    private val store = InMemoryStockStore()
    private val service = StockService(store, store, store, clock, StockPolicy(maxBatchSize = 10))

    private fun seed(onHand: Long, reserved: Long = 0, key: com.retail.stock.domain.model.StockKey = KEY_A1) =
        store.seed(StockLevel(key, onHand, reserved, version = 1, updatedAt = clock.instant()))

    private fun idem(value: String) = IdempotencyKey("test-key-$value")

    @Test
    fun `receiving stock creates the row and emits an event`() {
        val level = asActor(opsTeammate) {
            service.adjust(
                AdjustStock(
                    KEY_A1,
                    delta = 25,
                    reason = "receipt",
                    reference = "PO-1",
                    idempotencyKey = idem("1")
                )
            )
        }
        assertEquals(25, level.onHand)
        assertEquals(1, level.version)
        assertEquals(1, store.outbox.size)
        assertEquals(MovementType.ADJUSTMENT, store.outbox.single().type)
        assertEquals(25, store.outbox.single().onHandDelta)
    }

    @Test
    fun `cannot remove reserved or missing stock`() {
        seed(onHand = 10, reserved = 8)
        assertFailsWith<InsufficientStock> {
            asActor(opsTeammate) { service.adjust(AdjustStock(KEY_A1, -3, "damage", null, null)) }
        }
        assertFailsWith<InsufficientStock> {
            asActor(opsTeammate) { service.adjust(AdjustStock(KEY_B1, -1, "damage", null, null)) }
        }
        assertEquals(10, store.level(KEY_A1)!!.onHand)
        assertNull(store.level(KEY_B1), "failed transaction must not leave a created row behind")
        assertTrue(store.outbox.isEmpty())
    }

    @Test
    fun `idempotent replay returns the original result without re-applying`() {
        val command =
            AdjustStock(KEY_A1, delta = 5, reason = "receipt", reference = null, idempotencyKey = idem("replay"))
        val first = asActor(opsTeammate) { service.adjust(command) }
        val second = asActor(opsTeammate) { service.adjust(command) }
        assertEquals(first, second)
        assertEquals(5, store.level(KEY_A1)!!.onHand)
        assertEquals(1, store.outbox.size)
    }

    @Test
    fun `reusing an idempotency key with another payload is rejected`() {
        asActor(opsTeammate) { service.adjust(AdjustStock(KEY_A1, 5, "receipt", null, idem("conflict"))) }
        assertFailsWith<IdempotencyConflict> {
            asActor(opsTeammate) { service.adjust(AdjustStock(KEY_A1, 6, "receipt", null, idem("conflict"))) }
        }
    }

    @Test
    fun `reserve then commit moves units out of on-hand and reserved`() {
        seed(onHand = 10)
        val reserved = asActor(partnerStore1) {
            service.reserve(ReserveStock(KEY_A1, 4, Duration.ofMinutes(5), "ORDER-9", idem("r1")))
        }
        assertEquals(4, reserved.level.reserved)
        assertEquals(6, reserved.level.available)

        val committed =
            asActor(partnerStore1) { service.commit(CommitReservation(reserved.reservation.id, idem("c1"))) }
        assertEquals(ReservationStatus.COMMITTED, committed.reservation.status)
        assertEquals(6, committed.level.onHand)
        assertEquals(0, committed.level.reserved)
        assertEquals(
            listOf(MovementType.RESERVATION_CREATED, MovementType.RESERVATION_COMMITTED),
            store.outbox.map { it.type },
        )
        assertFailsWith<ReservationNotActive> {
            asActor(partnerStore1) { service.release(ReleaseReservation(reserved.reservation.id, "late", null)) }
        }
    }

    @Test
    fun `cannot over-reserve`() {
        seed(onHand = 3)
        assertFailsWith<InsufficientStock> {
            asActor(partnerStore1) { service.reserve(ReserveStock(KEY_A1, 4, null, null, null)) }
        }
    }

    @Test
    fun `partners are confined to their locations (ABAC)`() {
        seed(onHand = 10, key = KEY_A2)
        assertFailsWith<AccessDenied> {
            asActor(partnerStore1) { service.reserve(ReserveStock(KEY_A2, 1, null, null, null)) }
        }
        assertFailsWith<AccessDenied> {
            asActor(partnerStore1) { service.adjust(AdjustStock(KEY_A1, 1, "receipt", null, null)) }
        }
    }

    @Test
    fun `reservations outside the actor scope are invisible`() {
        seed(onHand = 10, key = KEY_A2)
        val outcome = asActor(opsTeammate) { service.reserve(ReserveStock(KEY_A2, 2, null, null, null)) }
        assertFailsWith<ReservationNotFound> {
            asActor(partnerStore1) { service.getReservation(outcome.reservation.id) }
        }
    }

    @Test
    fun `expired reservations are released by the sweeper`() {
        seed(onHand = 10)
        val outcome =
            asActor(partnerStore1) { service.reserve(ReserveStock(KEY_A1, 7, Duration.ofMinutes(1), null, null)) }
        clock.advance(Duration.ofMinutes(2))

        assertFailsWith<ReservationNotActive> {
            asActor(partnerStore1) { service.commit(CommitReservation(outcome.reservation.id, null)) }
        }
        val processed = asActor(Actor.SYSTEM) { service.expireReservations(100) }
        assertEquals(1, processed)
        assertEquals(0, store.level(KEY_A1)!!.reserved)
        assertEquals(ReservationStatus.EXPIRED, store.reservation(outcome.reservation.id)!!.status)
        assertEquals(MovementType.RESERVATION_EXPIRED, store.outbox.last().type)
    }

    @Test
    fun `batch adjustments are atomic`() {
        seed(onHand = 5, key = KEY_A1)
        assertFailsWith<InsufficientStock> {
            asActor(opsTeammate) {
                service.adjustBatch(
                    BatchAdjustStock(
                        listOf(
                            AdjustmentLine(KEY_B1, 10, "receipt", null),
                            AdjustmentLine(KEY_A1, -6, "sale", null),
                        ),
                        idem("batch-fail"),
                    ),
                )
            }
        }
        assertNull(store.level(KEY_B1))
        assertEquals(5, store.level(KEY_A1)!!.onHand)

        val levels = asActor(opsTeammate) {
            service.adjustBatch(
                BatchAdjustStock(
                    listOf(
                        AdjustmentLine(KEY_B1, 10, "receipt", null),
                        AdjustmentLine(KEY_A1, -2, "sale", null),
                        AdjustmentLine(KEY_A1, -1, "sale", null),
                    ),
                    idem("batch-ok"),
                ),
            )
        }
        assertEquals(listOf(2L, 10L), levels.map { it.onHand })
        assertEquals(3, store.outbox.size)
        assertEquals(listOf(2L, 3L), store.outbox.filter { it.key == KEY_A1 }.map { it.stockVersion })
    }

    @Test
    fun `batch size limit is enforced`() {
        val lines = (1..11).map { AdjustmentLine(KEY_A1, 1, "receipt", null) }
        assertFailsWith<ValidationFailed> {
            asActor(opsTeammate) { service.adjustBatch(BatchAdjustStock(lines, null)) }
        }
    }

    @Test
    fun `sync sets absolute count and rejects stale counts`() {
        seed(onHand = 10)
        val synced = asActor(opsTeammate) {
            service.sync(SyncStock(KEY_A1, 7, "WMS", clock.instant(), idem("sync-1")))
        }
        assertEquals(7, synced.onHand)
        assertEquals(MovementType.SYNC, store.outbox.single().type)
        assertEquals(-3, store.outbox.single().onHandDelta)

        assertFailsWith<StaleStockCount> {
            asActor(opsTeammate) {
                service.sync(SyncStock(KEY_A1, 9, "WMS", Instant.parse("2020-01-01T00:00:00Z"), null))
            }
        }
    }

    @Test
    fun `sync below reserved quantity surfaces an oversell`() {
        seed(onHand = 10, reserved = 6)
        val synced = asActor(opsTeammate) { service.sync(SyncStock(KEY_A1, 4, "cycle-count", null, null)) }
        assertEquals(-2, synced.available)
        assertTrue(synced.oversold)
    }

    @Test
    fun `missing actor is denied`() {
        assertFailsWith<AccessDenied> {
            kotlinx.coroutines.runBlocking { service.getLevel(KEY_A1) }
        }
    }
}
