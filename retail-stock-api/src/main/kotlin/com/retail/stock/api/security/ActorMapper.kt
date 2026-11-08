package com.retail.stock.api.security

import com.auth0.jwt.interfaces.Payload
import com.retail.stock.api.config.SecurityConfig
import com.retail.stock.domain.error.ValidationFailed
import com.retail.stock.domain.model.LocationId
import com.retail.stock.domain.security.Actor
import com.retail.stock.domain.security.ActorKind
import com.retail.stock.domain.security.LocationScope
import com.retail.stock.domain.security.Permission
import org.slf4j.LoggerFactory

/** Ktor principal wrapping the domain [Actor]. */
data class StockPrincipal(val actor: Actor)

/**
 * Maps a verified Keycloak access token to a domain [Actor].
 *
 * Claims used:
 *  - `realm_access.roles`: actor kind (`teammate` | `partner` | `service`) and permissions (`stock:*`)
 *  - `location_groups`: ABAC scope from group membership (`/locations/STORE-001`, `/locations/ALL`)
 *  - `azp`: issuing client. Teammate tokens are only accepted from passkey-protected clients.
 */
object ActorMapper {
    private val log = LoggerFactory.getLogger(ActorMapper::class.java)
    private const val LOCATION_GROUP_PREFIX = "/locations/"
    private const val ALL_LOCATIONS = "ALL"

    fun fromPayload(payload: Payload, security: SecurityConfig): Actor? {
        val subject = payload.subject?.takeIf(String::isNotBlank) ?: return reject("missing sub")
        val clientId = payload.claimString("azp") ?: payload.claimString("client_id") ?: return reject("missing azp")
        val roles = realmRoles(payload)

        val kind = when {
            "service" in roles -> ActorKind.SERVICE
            "partner" in roles -> ActorKind.PARTNER
            "teammate" in roles -> ActorKind.TEAMMATE
            else -> return reject("no actor role for $subject")
        }
        if (kind == ActorKind.TEAMMATE && clientId !in security.teammateClients) {
            return reject("teammate token from non-passkey client '$clientId'")
        }

        val permissions = roles.mapNotNull(Permission::fromClaim).toSet()
        val displayName = when (kind) {
            ActorKind.TEAMMATE -> payload.claimString("preferred_username") ?: subject
            else -> clientId
        }
        return Actor(
            subject = subject,
            kind = kind,
            displayName = displayName,
            clientId = clientId,
            permissions = permissions,
            locationScope = locationScope(payload),
        )
    }

    private fun realmRoles(payload: Payload): Set<String> {
        val realmAccess = runCatching { payload.getClaim("realm_access").asMap() }.getOrNull() ?: return emptySet()
        val roles = realmAccess["roles"] as? List<*> ?: return emptySet()
        return roles.filterIsInstance<String>().toSet()
    }

    private fun locationScope(payload: Payload): LocationScope {
        val groups =
            runCatching { payload.getClaim("location_groups").asList(String::class.java) }.getOrNull().orEmpty()
        val names =
            groups.filter { it.startsWith(LOCATION_GROUP_PREFIX) }.map { it.removePrefix(LOCATION_GROUP_PREFIX) }
        if (ALL_LOCATIONS in names) return LocationScope.All
        val locations = names.mapNotNull { name ->
            try {
                LocationId(name)
            } catch (e: ValidationFailed) {
                log.warn("Ignoring invalid location group '{}'", name)
                null
            }
        }.toSet()
        return LocationScope.Only(locations)
    }

    private fun Payload.claimString(name: String): String? =
        runCatching { getClaim(name).asString() }.getOrNull()?.takeIf(String::isNotBlank)

    private fun reject(reason: String): Actor? {
        log.debug("Rejected token: {}", reason)
        return null
    }
}
