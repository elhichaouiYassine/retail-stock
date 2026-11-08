package com.retail.stock.infrastructure.resilience

import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.min

/** Exponential backoff with +/-20% jitter so that many pods do not retry in lock step. */
object Backoff {
    fun delayFor(attempt: Int, base: Duration, max: Duration): Duration {
        val exponent = (attempt - 1).coerceIn(0, 30)
        val raw = min(max.toMillis().toDouble(), base.toMillis().toDouble() * (1L shl exponent))
        val jitter = ThreadLocalRandom.current().nextDouble(0.8, 1.2)
        return Duration.ofMillis(min(max.toMillis().toDouble(), raw * jitter).toLong().coerceAtLeast(1))
    }
}
