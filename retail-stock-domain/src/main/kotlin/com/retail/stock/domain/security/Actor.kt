package com.retail.stock.domain.security

import com.retail.stock.domain.error.AccessDenied
import com.retail.stock.domain.model.LocationId
import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

enum class ActorKind { TEAMMATE, PARTNER, SERVICE, SYSTEM }

/** RBAC permissions. [claim] is the Keycloak realm role name. */
enum class Permission(val claim: String) {
    STOCK_READ("stock:read"),
    STOCK_WRITE("stock:write"),
    STOCK_RESERVE("stock:reserve"),
    STOCK_SYNC("stock:sync"),
    STOCK_ADMIN("stock:admin");

    companion object {
        private val byClaim = entries.associateBy { it.claim }
        fun fromClaim(claim: String): Permission? = byClaim[claim]
    }
}

/** ABAC attribute: which locations an actor may see or mutate. */
sealed interface LocationScope {
    fun allows(locationId: LocationId): Boolean

    data object All : LocationScope {
        override fun allows(locationId: LocationId): Boolean = true
    }

    data class Only(val locations: Set<LocationId>) : LocationScope {
        override fun allows(locationId: LocationId): Boolean = locationId in locations
    }
}

data class Actor(
    val subject: String,
    val kind: ActorKind,
    val displayName: String,
    val clientId: String,
    val permissions: Set<Permission>,
    val locationScope: LocationScope,
) {
    fun has(permission: Permission): Boolean =
        permission in permissions || Permission.STOCK_ADMIN in permissions

    /** Audit label stored on every movement, e.g. `PARTNER:partner-acme`. */
    val auditName: String get() = "${kind.name}:$displayName"

    companion object {
        val SYSTEM: Actor = Actor(
            subject = "system",
            kind = ActorKind.SYSTEM,
            displayName = "retail-stock",
            clientId = "retail-stock",
            permissions = Permission.entries.toSet(),
            locationScope = LocationScope.All,
        )
    }
}

/**
 * Carries the authenticated [Actor] through structured concurrency: the Ktor interceptor
 * installs it and every suspend call below (services, repositories) can read it without
 * threading a parameter through every signature.
 */
class ActorContext(val actor: Actor) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ActorContext>
}

suspend fun currentActor(): Actor =
    currentCoroutineContext()[ActorContext]?.actor ?: throw AccessDenied("No authenticated actor in context")

object AccessPolicy {
    fun requirePermission(actor: Actor, permission: Permission) {
        if (!actor.has(permission)) throw AccessDenied("Missing permission '${permission.claim}'")
    }

    fun requireLocation(actor: Actor, locationId: LocationId) {
        if (!actor.locationScope.allows(locationId)) {
            throw AccessDenied("Actor '${actor.displayName}' is not allowed to access location ${locationId.value}")
        }
    }

    fun require(actor: Actor, permission: Permission, locationId: LocationId) {
        requirePermission(actor, permission)
        requireLocation(actor, locationId)
    }
}
