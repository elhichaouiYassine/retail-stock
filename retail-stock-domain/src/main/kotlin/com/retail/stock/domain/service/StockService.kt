package com.retail.stock.domain.service

import com.retail.stock.domain.command.*
import com.retail.stock.domain.error.*
import com.retail.stock.domain.model.*
import com.retail.stock.domain.port.*
import com.retail.stock.domain.security.AccessPolicy
import com.retail.stock.domain.security.Actor
import com.retail.stock.domain.security.Permission
import com.retail.stock.domain.security.currentActor
import java.time.Clock
import java.time.Duration
import java.time.Instant

data class StockPolicy(
    val defaultReservationTtl: Duration = Duration.ofMinutes(15),
    val maxReservationTtl: Duration = Duration.ofHours(24),
    val maxBatchSize: Int = 500,
    val maxQueryLimit: Int = 500,
    val maxReasonLength: Int = 256,
)

/**
 * Application service for every inventory mutation.
 *
 * Each public mutation is a single database transaction that:
 *  1. claims the idempotency key (replays the stored result on retries),
 *  2. locks the affected stock rows in deterministic order (`SELECT ... FOR UPDATE`),
 *  3. applies pure domain transitions on [StockLevel] / [Reservation],
 *  4. appends ledger movements and outbox events atomically with the state change.
 *
 * Authorization is enforced here as well as at the HTTP edge (defense in depth): the edge checks
 * route permissions and path locations, the service checks every location actually touched,
 * including those inside request bodies and resolved reservations.
 */
class StockService(
    private val transactions: StockTransactions,
    private val queries: StockQueries,
    private val codec: OperationResultCodec,
    private val clock: Clock = Clock.systemUTC(),
    private val policy: StockPolicy = StockPolicy(),
) {

    // ---------------------------------------------------------------- queries

    suspend fun getLevel(key: StockKey): StockLevel {
        AccessPolicy.require(currentActor(), Permission.STOCK_READ, key.locationId)
        return queries.findLevel(key) ?: throw StockLevelNotFound(key.sku.value, key.locationId.value)
    }

    suspend fun getLevelsBySku(sku: Sku, limit: Int): List<StockLevel> {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_READ)
        return queries.findLevelsBySku(sku, clampLimit(limit)).filter { actor.locationScope.allows(it.key.locationId) }
    }

    suspend fun getReservation(id: ReservationId): Reservation {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_READ)
        val reservation = queries.findReservation(id) ?: throw ReservationNotFound(id.toString())
        // Do not leak existence of reservations in locations outside the actor's scope.
        if (!actor.locationScope.allows(reservation.key.locationId)) throw ReservationNotFound(id.toString())
        return reservation
    }

    suspend fun getMovements(key: StockKey, limit: Int, before: Instant?): List<StockMovement> {
        AccessPolicy.require(currentActor(), Permission.STOCK_READ, key.locationId)
        return queries.findMovements(key, clampLimit(limit), before)
    }

    // ---------------------------------------------------------------- mutations

    suspend fun adjust(command: AdjustStock): StockLevel {
        val actor = currentActor()
        AccessPolicy.require(actor, Permission.STOCK_WRITE, command.key.locationId)
        validateLine(AdjustmentLine(command.key, command.delta, command.reason, command.reference))

        val result = execute(actor, command) { now ->
            val level = lockStockLevels(listOf(command.key), createMissing = command.delta > 0, now = now)[command.key]
                ?: throw InsufficientStock(command.key.sku.value, command.key.locationId.value, -command.delta, 0)
            val updated = level.adjust(command.delta, now)
            persist(
                levels = listOf(updated),
                movements = listOf(
                    movement(
                        before = level, after = updated, type = MovementType.ADJUSTMENT, actor = actor, now = now,
                        reason = command.reason, reference = command.reference, reservationId = null, command = command,
                    ),
                ),
            )
            OperationResult.Levels(listOf(updated))
        }
        return (result as OperationResult.Levels).levels.single()
    }

    suspend fun adjustBatch(command: BatchAdjustStock): List<StockLevel> {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_WRITE)
        if (command.lines.isEmpty()) throw ValidationFailed("Batch must contain at least one line")
        if (command.lines.size > policy.maxBatchSize) {
            throw ValidationFailed("Batch size ${command.lines.size} exceeds maximum ${policy.maxBatchSize}")
        }
        command.lines.forEach { line ->
            AccessPolicy.requireLocation(actor, line.key.locationId)
            validateLine(line)
        }

        val result = execute(actor, command) { now ->
            val keys = command.lines.map { it.key }.toSortedSet()
            val creatable = command.lines.filter { it.delta > 0 }.map { it.key }.toSet()
            val locked = lockStockLevels(keys, createMissing = creatable.isNotEmpty(), now = now)
            val current = LinkedHashMap(locked)
            val movements = ArrayList<StockMovement>(command.lines.size)

            for ((key, delta, reason, reference) in command.lines) {
                val before = current[key]
                    ?: throw InsufficientStock(key.sku.value, key.locationId.value, -delta, 0)
                val after = before.adjust(delta, now)
                current[key] = after
                movements += movement(
                    before = before, after = after, type = MovementType.ADJUSTMENT, actor = actor, now = now,
                    reason = reason, reference = reference, reservationId = null, command = command,
                )
            }
            // Rows created only to satisfy lock ordering but never touched are left at version 0.
            val changed = current.values.filter { it.version != (locked[it.key]?.version ?: -1) }
            persist(levels = changed, movements = movements)
            OperationResult.Levels(keys.map { current.getValue(it) })
        }
        return (result as OperationResult.Levels).levels
    }

    /** Absolute physical count from a store/WMS. Rejects counts older than the last change. */
    suspend fun sync(command: SyncStock): StockLevel {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_SYNC)
        AccessPolicy.requireLocation(actor, command.key.locationId)
        if (command.countedOnHand < 0) throw ValidationFailed("Counted on-hand quantity must be >= 0")
        validateText("source", command.source)

        val result = execute(actor, command) { now ->
            val level = lockStockLevels(listOf(command.key), createMissing = true, now = now).getValue(command.key)
            val countedAt = command.countedAt
            if (countedAt != null && level.version > 0 && countedAt.isBefore(level.updatedAt)) {
                throw StaleStockCount(
                    command.key.sku.value,
                    command.key.locationId.value,
                    countedAt.toString(),
                    level.updatedAt.toString(),
                )
            }
            if (level.version > 0 && level.onHand == command.countedOnHand) {
                OperationResult.Levels(listOf(level))
            } else {
                val updated = level.syncTo(command.countedOnHand, now)
                persist(
                    levels = listOf(updated),
                    movements = listOf(
                        movement(
                            before = level, after = updated, type = MovementType.SYNC, actor = actor, now = now,
                            reason = "count:${command.source}", reference = countedAt?.toString(),
                            reservationId = null, command = command,
                        ),
                    ),
                )
                OperationResult.Levels(listOf(updated))
            }
        }
        return (result as OperationResult.Levels).levels.single()
    }

    suspend fun reserve(command: ReserveStock): OperationResult.ReservationOutcome {
        val actor = currentActor()
        AccessPolicy.require(actor, Permission.STOCK_RESERVE, command.key.locationId)
        if (command.quantity <= 0) throw ValidationFailed("Reservation quantity must be > 0")
        command.reference?.let { validateText("reference", it) }
        val ttl = resolveTtl(command.ttl)

        val result = execute(actor, command) { now ->
            val level = lockStockLevels(listOf(command.key), createMissing = false, now = now)[command.key]
                ?: throw InsufficientStock(command.key.sku.value, command.key.locationId.value, command.quantity, 0)
            val updated = level.reserve(command.quantity, now)
            val reservation = Reservation(
                id = ReservationId(UuidV7.next(now.toEpochMilli())),
                key = command.key,
                quantity = command.quantity,
                status = ReservationStatus.ACTIVE,
                reference = command.reference,
                createdBy = actor.auditName,
                expiresAt = now.plus(ttl),
                createdAt = now,
                updatedAt = now,
            )
            insertReservation(reservation)
            persist(
                levels = listOf(updated),
                movements = listOf(
                    movement(
                        before = level, after = updated, type = MovementType.RESERVATION_CREATED, actor = actor,
                        now = now, reason = null, reference = command.reference, reservationId = reservation.id,
                        command = command,
                    ),
                ),
            )
            OperationResult.ReservationOutcome(reservation, updated)
        }
        return result as OperationResult.ReservationOutcome
    }

    suspend fun commit(command: CommitReservation): OperationResult.ReservationOutcome =
        completeReservation(command, command.reservationId, ReservationStatus.COMMITTED, reason = null)

    suspend fun release(command: ReleaseReservation): OperationResult.ReservationOutcome {
        command.reason?.let { validateText("reason", it) }
        return completeReservation(command, command.reservationId, ReservationStatus.RELEASED, command.reason)
    }

    /** Background sweeper: releases expired reservations. Returns the number processed. */
    suspend fun expireReservations(batchSize: Int): Int {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_ADMIN)
        return transactions.inTransaction {
            val now = clock.instant()
            val expired = lockExpiredReservations(now, batchSize)
            if (expired.isEmpty()) return@inTransaction 0

            val keys = expired.map { it.key }.toSortedSet()
            val current = LinkedHashMap(lockStockLevels(keys, createMissing = false, now = now))
            val movements = ArrayList<StockMovement>(expired.size)
            val closed = ArrayList<Reservation>(expired.size)

            for (reservation in expired) {
                val before = current[reservation.key]
                    ?: error("Stock row missing for active reservation ${reservation.id}")
                val after = before.release(reservation.quantity, now)
                current[reservation.key] = after
                closed += reservation.transitionTo(ReservationStatus.EXPIRED, now)
                movements += movement(
                    before = before, after = after, type = MovementType.RESERVATION_EXPIRED, actor = actor, now = now,
                    reason = "ttl-expired", reference = reservation.reference, reservationId = reservation.id,
                    command = null,
                )
            }
            updateReservations(closed)
            persist(levels = current.values, movements = movements)
            expired.size
        }
    }

    // ---------------------------------------------------------------- internals

    private suspend fun completeReservation(
        command: StockCommand,
        reservationId: ReservationId,
        target: ReservationStatus,
        reason: String?,
    ): OperationResult.ReservationOutcome {
        val actor = currentActor()
        AccessPolicy.requirePermission(actor, Permission.STOCK_RESERVE)

        val result = execute(actor, command) { now ->
            // Lock order is always reservation -> stock row (same as the expiry sweeper): no lock cycles.
            val reservation = lockReservation(reservationId) ?: throw ReservationNotFound(reservationId.toString())
            if (!actor.locationScope.allows(reservation.key.locationId)) throw ReservationNotFound(reservationId.toString())
            if (reservation.status != ReservationStatus.ACTIVE) {
                throw ReservationNotActive(reservationId.toString(), reservation.status.name)
            }
            if (target == ReservationStatus.COMMITTED && reservation.isExpiredAt(now)) {
                throw ReservationNotActive(reservationId.toString(), ReservationStatus.EXPIRED.name)
            }

            val level = lockStockLevels(listOf(reservation.key), createMissing = false, now = now)[reservation.key]
                ?: error("Stock row missing for active reservation $reservationId")
            val (updated, type) = when (target) {
                ReservationStatus.COMMITTED -> level.commit(
                    reservation.quantity,
                    now
                ) to MovementType.RESERVATION_COMMITTED

                ReservationStatus.RELEASED -> level.release(
                    reservation.quantity,
                    now
                ) to MovementType.RESERVATION_RELEASED

                else -> throw IllegalArgumentException("Unsupported reservation transition to $target")
            }
            val closed = reservation.transitionTo(target, now)
            updateReservations(listOf(closed))
            persist(
                levels = listOf(updated),
                movements = listOf(
                    movement(
                        before = level, after = updated, type = type, actor = actor, now = now, reason = reason,
                        reference = reservation.reference, reservationId = reservation.id, command = command,
                    ),
                ),
            )
            OperationResult.ReservationOutcome(closed, updated)
        }
        return result as OperationResult.ReservationOutcome
    }

    private suspend fun execute(
        actor: Actor,
        command: StockCommand,
        block: suspend StockTransaction.(Instant) -> OperationResult,
    ): OperationResult = transactions.inTransaction {
        val key = command.idempotencyKey
        val scope = actor.subject
        val fingerprint = command.fingerprint()
        if (key != null) {
            when (val claim = claimIdempotencyKey(scope, key, fingerprint)) {
                is IdempotencyClaim.Replay -> {
                    if (claim.fingerprint != fingerprint) throw IdempotencyConflict(key.value)
                    return@inTransaction codec.decode(claim.payload)
                }

                IdempotencyClaim.Claimed -> Unit
            }
        }
        val result = block(clock.instant())
        if (key != null) storeIdempotentResult(scope, key, codec.encode(result))
        result
    }

    private suspend fun StockTransaction.persist(levels: Collection<StockLevel>, movements: List<StockMovement>) {
        if (levels.isNotEmpty()) saveStockLevels(levels)
        if (movements.isNotEmpty()) {
            appendMovements(movements)
            enqueueEvents(movements.map(StockEvent::of))
        }
    }

    private fun movement(
        before: StockLevel,
        after: StockLevel,
        type: MovementType,
        actor: Actor,
        now: Instant,
        reason: String?,
        reference: String?,
        reservationId: ReservationId?,
        command: StockCommand?,
    ): StockMovement = StockMovement(
        id = UuidV7.next(now.toEpochMilli()),
        key = after.key,
        type = type,
        onHandDelta = after.onHand - before.onHand,
        reservedDelta = after.reserved - before.reserved,
        onHandAfter = after.onHand,
        reservedAfter = after.reserved,
        stockVersion = after.version,
        reason = reason,
        reference = reference,
        reservationId = reservationId,
        actor = actor.auditName,
        idempotencyKey = command?.idempotencyKey?.value,
        occurredAt = now,
    )

    private fun validateLine(line: AdjustmentLine) {
        if (line.delta == 0L) throw ValidationFailed("Adjustment delta must not be zero for ${line.key}")
        validateText("reason", line.reason)
        line.reference?.let { validateText("reference", it) }
    }

    private fun validateText(field: String, value: String) {
        if (value.isBlank()) throw ValidationFailed("'$field' must not be blank")
        if (value.length > policy.maxReasonLength) {
            throw ValidationFailed("'$field' must be at most ${policy.maxReasonLength} characters")
        }
    }

    private fun resolveTtl(requested: Duration?): Duration {
        val ttl = requested ?: policy.defaultReservationTtl
        if (ttl.isNegative || ttl.isZero) throw ValidationFailed("Reservation TTL must be positive")
        if (ttl > policy.maxReservationTtl) {
            throw ValidationFailed("Reservation TTL must be at most ${policy.maxReservationTtl.toMinutes()} minutes")
        }
        return ttl
    }

    private fun clampLimit(limit: Int): Int = limit.coerceIn(1, policy.maxQueryLimit)
}
