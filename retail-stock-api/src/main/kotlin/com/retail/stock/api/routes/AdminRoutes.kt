package com.retail.stock.api.routes

import com.retail.stock.api.dto.DeadLettersResponse
import com.retail.stock.api.dto.ReplayResponse
import com.retail.stock.api.dto.toResponse
import com.retail.stock.api.security.secured
import com.retail.stock.domain.error.ValidationFailed
import com.retail.stock.domain.security.Permission
import com.retail.stock.infrastructure.dlq.DeadLetterService
import com.retail.stock.infrastructure.dlq.DeadLetterStatus
import com.retail.stock.infrastructure.dlq.ReplayResult
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.plugins.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

/** Dead letter queue operations. `stock:admin` only. */
fun Route.adminRoutes(deadLetters: DeadLetterService) {
    secured(HttpMethod.Get, "/admin/dead-letters", Permission.STOCK_ADMIN) {
        val status = call.request.queryParameters["status"]?.let { raw ->
            DeadLetterStatus.entries.firstOrNull { it.name.equals(raw, ignoreCase = true) }
                ?: throw ValidationFailed("status must be one of ${DeadLetterStatus.entries}")
        } ?: DeadLetterStatus.PENDING
        val limit = call.queryInt("limit", 50).coerceIn(1, 500)
        val afterId = call.request.queryParameters["afterId"]?.toLongOrNull()
        val items = deadLetters.list(status, limit, afterId)
        call.respond(
            DeadLettersResponse(
                items = items.map { it.toResponse() },
                nextAfterId = items.takeIf { it.size >= limit }?.lastOrNull()?.id,
            ),
        )
    }

    secured(HttpMethod.Get, "/admin/dead-letters/{id}", Permission.STOCK_ADMIN) {
        val id = call.deadLetterId()
        val letter = deadLetters.find(id) ?: throw NotFoundException("Dead letter $id not found")
        call.respond(letter.toResponse())
    }

    secured(HttpMethod.Post, "/admin/dead-letters/{id}/replay", Permission.STOCK_ADMIN) {
        val id = call.deadLetterId()
        when (val result = deadLetters.replay(id)) {
            ReplayResult.Replayed -> call.respond(ReplayResponse(id, "REPLAYED"))
            ReplayResult.NotFound -> throw NotFoundException("Dead letter $id not found")
            is ReplayResult.NotReplayable -> call.respond(
                HttpStatusCode.Conflict,
                ReplayResponse(id, "NOT_REPLAYABLE", result.reason)
            )

            is ReplayResult.Failed -> call.respond(
                HttpStatusCode.ServiceUnavailable,
                ReplayResponse(id, "FAILED", result.reason)
            )
        }
    }

    secured(HttpMethod.Post, "/admin/dead-letters/{id}/discard", Permission.STOCK_ADMIN) {
        val id = call.deadLetterId()
        if (deadLetters.discard(id)) {
            call.respond(ReplayResponse(id, "DISCARDED"))
        } else {
            call.respond(HttpStatusCode.Conflict, ReplayResponse(id, "NOT_PENDING"))
        }
    }
}

private fun ApplicationCall.deadLetterId(): Long =
    pathParam("id").toLongOrNull() ?: throw ValidationFailed("Dead letter id must be a number")
