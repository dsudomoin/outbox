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

    override fun handle(record: OutboxRecord) {
        control.apply(ROUTE, record.keyAsString())
        postOne(target, ROUTE, record)
    }
}

/** 422 is the target's permanent refusal, any other failure a retry. */
internal fun postOne(target: TargetHttp, sink: String, record: OutboxRecord) {
    val status = target.post(sink, record)
    if (status == 422) throw OutboxPermanentException("target rejected ${record.messageId()}")
    check(status == 200) { "target answered $status" }
}
