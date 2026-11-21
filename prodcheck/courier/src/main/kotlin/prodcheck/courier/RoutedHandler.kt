package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxPermanentException
import io.github.dsudomoin.outbox.OutboxRecord
import io.github.dsudomoin.outbox.OutboxTopicHandler
import prodcheck.common.HandlerControl
import ru.tinkoff.kora.common.Component

@Component
class RoutedHandler(private val target: TargetHttp, private val control: HandlerControl) : OutboxTopicHandler {

    companion object {
        const val ROUTE = "routed"
    }

    override fun topics(): Set<String> = setOf(ROUTE)

    override fun handle(record: OutboxRecord) = postOne(target, control, ROUTE, record)
}

/**
 * The route's handler control, then the post to its sink: 422 is the target's permanent refusal, any other failure a
 * retry.
 */
internal fun postOne(target: TargetHttp, control: HandlerControl, route: String, record: OutboxRecord) {
    control.apply(route, record.keyAsString())
    val status = target.post(route, record)
    if (status == 422) throw OutboxPermanentException("target rejected ${record.messageId()}")
    check(status == 200) { "target answered $status" }
}
