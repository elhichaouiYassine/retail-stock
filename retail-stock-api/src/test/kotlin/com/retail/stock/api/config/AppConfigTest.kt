package com.retail.stock.api.config

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AppConfigTest {
    @Test
    fun `defaults work with an empty environment`() {
        val config = AppConfig.load(emptyMap(), emptyMap())
        assertEquals(8080, config.http.port)
        assertEquals("retail-stock-api", config.security.audience)
        assertEquals("http://localhost:8180/realms/retail-stock/protocol/openid-connect/certs", config.security.jwksUrl)
    }

    @Test
    fun `environment overrides defaults`() {
        val config = AppConfig.load(
            mapOf(
                "APP_PORT" to "9090",
                "OUTBOX_POLL_INTERVAL" to "50ms",
                "KAFKA_AUTO_CREATE_TOPICS" to "false"
            ), emptyMap()
        )
        assertEquals(9090, config.http.port)
        assertEquals(Duration.ofMillis(50), config.outbox.pollInterval)
        assertEquals(false, config.kafka.autoCreateTopics)
    }

    @Test
    fun `dotenv parser handles comments quotes and export`() {
        val parsed = DotEnv.parse(
            listOf(
                "# comment",
                "",
                "export A=1",
                "B=\"two words\"",
                "C='single'",
                "D=value # trailing comment",
                "E=",
            ),
        )
        assertEquals(mapOf("A" to "1", "B" to "two words", "C" to "single", "D" to "value", "E" to ""), parsed)
    }

    @Test
    fun `invalid values fail fast`() {
        assertFailsWith<IllegalArgumentException> { Env(listOf(mapOf("X" to "abc"))).int("X", 1) }
        assertFailsWith<IllegalArgumentException> { Env(listOf(mapOf("X" to "soon"))).duration("X", Duration.ZERO) }
    }
}
