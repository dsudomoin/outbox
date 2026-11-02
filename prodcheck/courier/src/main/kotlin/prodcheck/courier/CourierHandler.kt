package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.OutboxPermanentException
import io.github.dsudomoin.outbox.OutboxRecord
import io.github.dsudomoin.outbox.Outcome
import prodcheck.common.HandlerControl
import prodcheck.common.Payload

/**
 * One handler for all topics, one POST per round. A round carries at most one record per (topic, key): the target
 * answers item by item, so a later record of a key must not leave until the earlier one's answer is known. With
 * distinct keys in the call, that is one POST for the whole call.
 */
class CourierHandler(private val target: TargetHttp, private val control: HandlerControl) : OutboxHandler {

    companion object {
        const val ROUTE = "main"
    }

    override fun handle(record: OutboxRecord) {
        when (val outcome = handleBatch(listOf(record))?.single()) {
            is Outcome.Delivered -> return
            is Outcome.Permanent -> throw OutboxPermanentException("target rejected ${record.messageId()}", outcome.error())
            is Outcome.Retry -> throw outcome.error()
            else -> throw IllegalStateException("not delivered: $outcome")
        }
    }

    override fun handleBatch(records: List<OutboxRecord>): List<Outcome?>? {
        when (control.state(ROUTE).mode) {
            "throw" -> throw RuntimeException("handler mode throw")
            "error" -> throw Error("handler mode error")
            "null-list" -> return null
            "wrong-size" -> return records.drop(1).map { Outcome.delivered() }
            "null-element" -> return records.map { null }
        }

        val outcomes = arrayOfNulls<Outcome>(records.size)
        val queues = LinkedHashMap<Any, ArrayDeque<Int>>()
        records.forEachIndexed { i, record ->
            // a record without a key blocks nothing: a queue of its own
            val key: Any = record.keyAsString()?.let { record.topic() to it } ?: i
            queues.getOrPut(key) { ArrayDeque() }.addLast(i)
        }

        fun block(key: Any) {
            queues.remove(key)?.forEach { outcomes[it] = Outcome.notAttempted() }
        }

        while (queues.isNotEmpty() && !OutboxHandler.windDownRequested()) {
            val round = ArrayList<Int>()
            val roundKeys = ArrayList<Any>()
            try {
                for (key in queues.keys.toList()) {
                    val next = nextToSend(queues.getValue(key), records, outcomes)
                    when {
                        next == BLOCKED -> block(key)
                        next == NOTHING -> queues.remove(key)
                        else -> {
                            round.add(next)
                            roundKeys.add(key)
                        }
                    }
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                round.forEach { outcomes[it] = Outcome.notAttempted() }
                break
            }
            if (round.isEmpty()) continue
            val statuses = try {
                target.postBatch(round.map { records[it] })
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                round.forEach { outcomes[it] = Outcome.retry(e) }
                roundKeys.forEach { block(it) }
                break
            } catch (e: Exception) { // no per-item answer: the whole round failed
                round.forEach { outcomes[it] = Outcome.retry(e) }
                roundKeys.forEach { block(it) }
                continue
            }
            statuses.forEachIndexed { i, status ->
                outcomes[round[i]] = when (status) {
                    200 -> Outcome.delivered()
                    422 -> Outcome.permanent(RuntimeException("target rejected ${records[round[i]].messageId()}"))
                    else -> Outcome.retry(RuntimeException("target answered $status"))
                }
                if (status != 200 && status != 422) block(roundKeys[i])
            }
        }
        return outcomes.map { it ?: Outcome.notAttempted() }
    }

    /** Takes the queue's next record to post, deciding locally what needs no target; [BLOCKED] ends the key's call. */
    private fun nextToSend(queue: ArrayDeque<Int>, records: List<OutboxRecord>, outcomes: Array<Outcome?>): Int {
        val state = control.state(ROUTE)
        while (queue.isNotEmpty()) {
            val i = queue.first()
            val record = records[i]
            val tag = Payload.tag(record.payload())
            when {
                tag == "requeue" && record.requeues() < 3 -> {
                    outcomes[i] = Outcome.requeue() // later messages of the key go on
                    queue.removeFirst()
                }
                tag == "wait" && state.waiting -> {
                    outcomes[i] = Outcome.waitFor("courier waits")
                    queue.removeFirst()
                    return BLOCKED
                }
                tag == "retry3" && record.attempts() < 3 -> {
                    outcomes[i] = Outcome.retry(RuntimeException("retry3, attempts ${record.attempts()}"))
                    queue.removeFirst()
                    return BLOCKED
                }
                else -> {
                    try {
                        control.apply(ROUTE, record.keyAsString())
                    } catch (e: InterruptedException) {
                        throw e
                    } catch (e: Exception) {
                        outcomes[i] = Outcome.retry(e)
                        queue.removeFirst()
                        return BLOCKED
                    }
                    queue.removeFirst()
                    return i
                }
            }
        }
        return NOTHING
    }
}

private const val BLOCKED = -1
private const val NOTHING = -2
