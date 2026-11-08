package com.retail.stock.api.security

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.retail.stock.api.config.SecurityConfig
import com.retail.stock.domain.model.LocationId
import com.retail.stock.domain.security.ActorKind
import com.retail.stock.domain.security.LocationScope
import com.retail.stock.domain.security.Permission
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ActorMapperTest {
    private val security = SecurityConfig(
        issuer = "http://localhost:8180/realms/retail-stock",
        jwksUrl = "http://localhost:8180/realms/retail-stock/protocol/openid-connect/certs",
        audience = "retail-stock-api",
        leewaySeconds = 5,
        teammateClients = setOf("retail-stock-web"),
    )

    private fun token(azp: String, roles: List<String>, groups: List<String>, username: String? = null) = JWT.decode(
        JWT.create()
            .withSubject("subject-1")
            .withClaim("azp", azp)
            .apply { if (username != null) withClaim("preferred_username", username) }
            .withClaim("realm_access", mapOf("roles" to roles))
            .withClaim("location_groups", groups)
            .sign(Algorithm.HMAC256("test-only-secret")),
    )

    @Test
    fun `partner token maps to scoped partner actor`() {
        val actor = assertNotNull(
            ActorMapper.fromPayload(
                token(
                    "partner-acme",
                    listOf("partner", "stock:read", "stock:reserve", "offline_access"),
                    listOf("/locations/STORE-001")
                ),
                security,
            ),
        )
        assertEquals(ActorKind.PARTNER, actor.kind)
        assertEquals("partner-acme", actor.displayName)
        assertEquals(setOf(Permission.STOCK_READ, Permission.STOCK_RESERVE), actor.permissions)
        assertEquals(LocationScope.Only(setOf(LocationId("STORE-001"))), actor.locationScope)
    }

    @Test
    fun `teammate token from the passkey client gets full scope`() {
        val actor = assertNotNull(
            ActorMapper.fromPayload(
                token(
                    "retail-stock-web",
                    listOf("teammate", "stock:admin"),
                    listOf("/locations/ALL"),
                    username = "alice"
                ),
                security,
            ),
        )
        assertEquals(ActorKind.TEAMMATE, actor.kind)
        assertEquals("alice", actor.displayName)
        assertEquals(LocationScope.All, actor.locationScope)
    }

    @Test
    fun `teammate token from a non passkey client is rejected`() {
        assertNull(ActorMapper.fromPayload(token("retail-stock-dev-cli", listOf("teammate"), emptyList()), security))
    }

    @Test
    fun `token without actor role is rejected`() {
        assertNull(ActorMapper.fromPayload(token("some-client", listOf("stock:read"), emptyList()), security))
    }
}
