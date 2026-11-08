package com.retail.stock.domain.support

import com.retail.stock.domain.model.LocationId
import com.retail.stock.domain.model.Sku
import com.retail.stock.domain.model.StockKey
import com.retail.stock.domain.security.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.time.*

class MutableClock(private var now: Instant = Instant.parse("2026-11-04T10:00:00Z")) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId?): Clock = this
    override fun instant(): Instant = now
    fun advance(duration: Duration) {
        now = now.plus(duration)
    }
}

object Fixtures {
    val STORE_1 = LocationId("STORE-001")
    val STORE_2 = LocationId("STORE-002")
    val SKU_A = Sku("SKU-A")
    val SKU_B = Sku("SKU-B")
    val KEY_A1 = StockKey(SKU_A, STORE_1)
    val KEY_B1 = StockKey(SKU_B, STORE_1)
    val KEY_A2 = StockKey(SKU_A, STORE_2)

    val opsTeammate = Actor(
        subject = "teammate-1",
        kind = ActorKind.TEAMMATE,
        displayName = "alice",
        clientId = "retail-stock-web",
        permissions = setOf(
            Permission.STOCK_READ,
            Permission.STOCK_WRITE,
            Permission.STOCK_RESERVE,
            Permission.STOCK_SYNC
        ),
        locationScope = LocationScope.All,
    )

    val partnerStore1 = Actor(
        subject = "partner-acme",
        kind = ActorKind.PARTNER,
        displayName = "partner-acme",
        clientId = "partner-acme",
        permissions = setOf(Permission.STOCK_READ, Permission.STOCK_RESERVE),
        locationScope = LocationScope.Only(setOf(STORE_1)),
    )
}

fun <T> asActor(actor: Actor, block: suspend () -> T): T = runBlocking {
    withContext(ActorContext(actor)) { block() }
}
