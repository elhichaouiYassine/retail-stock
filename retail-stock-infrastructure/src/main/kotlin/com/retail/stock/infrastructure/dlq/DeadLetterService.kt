package com.retail.stock.infrastructure.dlq

import com.retail.stock.infrastructure.kafka.StockSyncHandler
import org.slf4j.LoggerFactory

sealed interface ReplayResult {
    data object Replayed : ReplayResult
    data object NotFound : ReplayResult
    data class NotReplayable(val reason: String) : ReplayResult
    data class Failed(val reason: String) : ReplayResult
}

/**
 * Operator actions on the PostgreSQL dead letter queue.
 * - OUTBOX letters are re-queued into the outbox (relay publishes them again with the same event id).
 * - CONSUMER letters are re-applied through the same handler the Kafka consumer uses.
 */
class DeadLetterService(
    private val repository: DeadLetterRepository,
    private val syncHandler: StockSyncHandler,
) {
    private val log = LoggerFactory.getLogger(DeadLetterService::class.java)

    suspend fun list(status: DeadLetterStatus, limit: Int, afterId: Long?): List<DeadLetter> =
        repository.list(status, limit.coerceIn(1, 500), afterId)

    suspend fun find(id: Long): DeadLetter? = repository.find(id)

    suspend fun discard(id: Long): Boolean =
        repository.markStatus(id, DeadLetterStatus.PENDING, DeadLetterStatus.DISCARDED)

    suspend fun replay(id: Long): ReplayResult {
        val letter = repository.find(id) ?: return ReplayResult.NotFound
        if (letter.status != DeadLetterStatus.PENDING) return ReplayResult.NotReplayable("Dead letter is ${letter.status}")
        return when (letter.source) {
            DeadLetterSource.OUTBOX ->
                if (repository.requeueToOutbox(id)) ReplayResult.Replayed else ReplayResult.NotReplayable("Concurrent update or missing event id")

            DeadLetterSource.CONSUMER -> try {
                syncHandler.handle(letter.payload)
                if (repository.markStatus(id, DeadLetterStatus.PENDING, DeadLetterStatus.REPLAYED)) {
                    ReplayResult.Replayed
                } else {
                    ReplayResult.NotReplayable("Concurrent update")
                }
            } catch (e: StockSyncHandler.PoisonMessage) {
                ReplayResult.NotReplayable(e.message ?: "poison message")
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("Replay of dead letter {} failed", id, e)
                ReplayResult.Failed(e.toString())
            }
        }
    }
}
