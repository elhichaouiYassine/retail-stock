package com.retail.stock.infrastructure.serialization

import com.retail.stock.domain.model.*
import com.retail.stock.domain.port.OperationResult
import com.retail.stock.domain.port.OperationResultCodec
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.time.Instant
import java.util.*

val StockJson: Json = Json {
    encodeDefaults = true
    ignoreUnknownKeys = true
}

/** Wire contract of `retail.stock.events.v1` (schemaVersion 1). Additive changes only. */
@Serializable
data class StockEventMessage(
    val schemaVersion: Int = 1,
    val eventId: String,
    val eventType: String,
    val sku: String,
    val locationId: String,
    val stockVersion: Long,
    val onHandDelta: Long,
    val reservedDelta: Long,
    val onHand: Long,
    val reserved: Long,
    val available: Long,
    val reservationId: String? = null,
    val reason: String? = null,
    val reference: String? = null,
    val actor: String,
    val occurredAt: String,
)

/** Wire contract of `retail.stock.sync.v1`: absolute counts pushed by WMS / store systems. */
@Serializable
data class StockSyncMessage(
    val eventId: String,
    val sku: String,
    val locationId: String,
    val onHand: Long,
    val source: String,
    val countedAt: String? = null,
)

object EventCodec {
    fun encode(event: StockEvent): String = StockJson.encodeToString(
        StockEventMessage.serializer(),
        StockEventMessage(
            eventId = event.eventId.toString(),
            eventType = event.type.name,
            sku = event.key.sku.value,
            locationId = event.key.locationId.value,
            stockVersion = event.stockVersion,
            onHandDelta = event.onHandDelta,
            reservedDelta = event.reservedDelta,
            onHand = event.onHand,
            reserved = event.reserved,
            available = event.available,
            reservationId = event.reservationId?.toString(),
            reason = event.reason,
            reference = event.reference,
            actor = event.actor,
            occurredAt = event.occurredAt.toString(),
        ),
    )

    fun decode(payload: String): StockEvent {
        val m = StockJson.decodeFromString(StockEventMessage.serializer(), payload)
        return StockEvent(
            eventId = UUID.fromString(m.eventId),
            type = MovementType.valueOf(m.eventType),
            key = StockKey(Sku(m.sku), LocationId(m.locationId)),
            stockVersion = m.stockVersion,
            onHandDelta = m.onHandDelta,
            reservedDelta = m.reservedDelta,
            onHand = m.onHand,
            reserved = m.reserved,
            available = m.available,
            reservationId = m.reservationId?.let { ReservationId(UUID.fromString(it)) },
            reason = m.reason,
            reference = m.reference,
            actor = m.actor,
            occurredAt = Instant.parse(m.occurredAt),
        )
    }

    fun decodeSync(payload: String): StockSyncMessage =
        StockJson.decodeFromString(StockSyncMessage.serializer(), payload)

    fun encodeSync(message: StockSyncMessage): String =
        StockJson.encodeToString(StockSyncMessage.serializer(), message)
}

// ------------------------------------------------------------------ idempotent result snapshots

@Serializable
internal data class LevelSnapshot(
    val sku: String,
    val locationId: String,
    val onHand: Long,
    val reserved: Long,
    val version: Long,
    val updatedAt: String,
)

@Serializable
internal data class ReservationSnapshot(
    val id: String,
    val sku: String,
    val locationId: String,
    val quantity: Long,
    val status: String,
    val reference: String? = null,
    val createdBy: String,
    val expiresAt: String,
    val createdAt: String,
    val updatedAt: String,
)

@Serializable
internal data class ResultEnvelope(
    val kind: String,
    val levels: List<LevelSnapshot> = emptyList(),
    val reservation: ReservationSnapshot? = null,
)

object JsonOperationResultCodec : OperationResultCodec {
    private const val LEVELS = "levels"
    private const val RESERVATION = "reservation"

    override fun encode(result: OperationResult): String {
        val envelope = when (result) {
            is OperationResult.Levels -> ResultEnvelope(LEVELS, levels = result.levels.map { it.snapshot() })
            is OperationResult.ReservationOutcome -> ResultEnvelope(
                RESERVATION,
                levels = listOf(result.level.snapshot()),
                reservation = result.reservation.snapshot(),
            )
        }
        return StockJson.encodeToString(ResultEnvelope.serializer(), envelope)
    }

    override fun decode(payload: String): OperationResult {
        val envelope = StockJson.decodeFromString(ResultEnvelope.serializer(), payload)
        return when (envelope.kind) {
            LEVELS -> OperationResult.Levels(envelope.levels.map { it.toDomain() })
            RESERVATION -> OperationResult.ReservationOutcome(
                reservation = checkNotNull(envelope.reservation) { "reservation snapshot missing" }.toDomain(),
                level = envelope.levels.single().toDomain(),
            )

            else -> error("Unknown idempotent result kind '${envelope.kind}'")
        }
    }

    private fun StockLevel.snapshot() = LevelSnapshot(
        sku = key.sku.value,
        locationId = key.locationId.value,
        onHand = onHand,
        reserved = reserved,
        version = version,
        updatedAt = updatedAt.toString(),
    )

    private fun LevelSnapshot.toDomain() = StockLevel(
        key = StockKey(Sku(sku), LocationId(locationId)),
        onHand = onHand,
        reserved = reserved,
        version = version,
        updatedAt = Instant.parse(updatedAt),
    )

    private fun Reservation.snapshot() = ReservationSnapshot(
        id = id.toString(),
        sku = key.sku.value,
        locationId = key.locationId.value,
        quantity = quantity,
        status = status.name,
        reference = reference,
        createdBy = createdBy,
        expiresAt = expiresAt.toString(),
        createdAt = createdAt.toString(),
        updatedAt = updatedAt.toString(),
    )

    private fun ReservationSnapshot.toDomain() = Reservation(
        id = ReservationId(UUID.fromString(id)),
        key = StockKey(Sku(sku), LocationId(locationId)),
        quantity = quantity,
        status = ReservationStatus.valueOf(status),
        reference = reference,
        createdBy = createdBy,
        expiresAt = Instant.parse(expiresAt),
        createdAt = Instant.parse(createdAt),
        updatedAt = Instant.parse(updatedAt),
    )
}
