package com.retail.stock.api.security

import com.auth0.jwk.JwkProviderBuilder
import com.retail.stock.api.config.SecurityConfig
import com.retail.stock.api.dto.ErrorResponse
import com.retail.stock.domain.error.AccessDenied
import com.retail.stock.domain.model.LocationId
import com.retail.stock.domain.security.AccessPolicy
import com.retail.stock.domain.security.ActorContext
import com.retail.stock.domain.security.Permission
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.withContext
import java.net.URI
import java.util.concurrent.TimeUnit

const val AUTH_KEYCLOAK = "keycloak-jwt"

/**
 * OAuth2 / OIDC bearer validation against Keycloak.
 *
 * Teammates authenticate in Keycloak with passkeys (WebAuthn / FIDO2, see the realm's passwordless policy);
 * partners and services use the client-credentials grant. Either way this service only ever sees a signed
 * JWT: signature (JWKS, cached + rate limited), issuer, audience and expiry are verified here, then the
 * claims are mapped to an [com.retail.stock.domain.security.Actor].
 */
fun Application.configureSecurity(config: SecurityConfig) {
    val jwkProvider = JwkProviderBuilder(URI(config.jwksUrl).toURL())
        .cached(10, 12, TimeUnit.HOURS)
        .rateLimited(20, 1, TimeUnit.MINUTES)
        .timeouts(2_000, 2_000)
        .build()

    install(Authentication) {
        jwt(AUTH_KEYCLOAK) {
            realm = "retail-stock"
            verifier(jwkProvider, config.issuer) {
                acceptLeeway(config.leewaySeconds)
                withAudience(config.audience)
            }
            validate { credential -> ActorMapper.fromPayload(credential.payload, config)?.let(::StockPrincipal) }
            challenge { _, _ ->
                call.respond(
                    HttpStatusCode.Unauthorized,
                    ErrorResponse("UNAUTHORIZED", "A valid Keycloak bearer token is required", traceId = call.callId),
                )
            }
        }
    }
}

class AccessRequirement {
    var permissions: Set<Permission> = emptySet()
    var locationParameter: String? = null
}

/**
 * Route-scoped RBAC + ABAC interceptor. Runs right after authentication, before the handler:
 * - RBAC: the actor must hold every required permission (`stock:admin` implies all),
 * - ABAC: if the route carries a location path parameter, the actor's location scope must include it.
 * Failures throw [AccessDenied], rendered as 403 by StatusPages.
 */
val RequireAccess = createRouteScopedPlugin("RequireAccess", ::AccessRequirement) {
    val permissions = pluginConfig.permissions
    val locationParameter = pluginConfig.locationParameter

    on(AuthenticationChecked) { call ->
        if (call.response.isCommitted) return@on
        val actor = call.principal<StockPrincipal>()?.actor ?: throw AccessDenied("Authentication required")
        permissions.forEach { AccessPolicy.requirePermission(actor, it) }
        if (locationParameter != null) {
            val raw = call.parameters[locationParameter]
            if (raw != null) AccessPolicy.requireLocation(actor, LocationId(raw))
        }
    }
}

/** Declares an endpoint guarded by [RequireAccess]. Each endpoint gets its own route node, so policies never leak. */
fun Route.secured(
    method: HttpMethod,
    path: String,
    vararg permissions: Permission,
    locationParameter: String? = null,
    handler: suspend RoutingContext.() -> Unit,
) {
    val required = permissions.toSet()
    val locationParam = locationParameter
    route(path, method) {
        install(RequireAccess) {
            this.permissions = required
            this.locationParameter = locationParam
        }
        handle(handler)
    }
}

/**
 * Propagates the authenticated actor into the coroutine context for everything the handler calls:
 * the domain service re-checks ABAC on body/resolved locations and stamps the actor on every movement.
 */
suspend fun <T> RoutingContext.asActor(block: suspend () -> T): T {
    val actor = call.principal<StockPrincipal>()?.actor ?: throw AccessDenied("Authentication required")
    return withContext(ActorContext(actor)) { block() }
}
