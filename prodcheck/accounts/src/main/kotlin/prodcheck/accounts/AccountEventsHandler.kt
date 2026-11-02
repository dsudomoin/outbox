package prodcheck.accounts

import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.OutboxRecord
import io.github.dsudomoin.outbox.OutboxTopicHandler
import io.github.dsudomoin.outbox.Outcome
import prodcheck.common.HandlerControl
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.nio.ByteBuffer
import java.sql.Connection

@Component
class AccountEventsHandler(
    private val operation: AccountOperation,
    private val control: HandlerControl,
    private val db: JdbcConnectionFactory,
) : OutboxTopicHandler {
    override fun topics(): Set<String> =
        setOf(ROUTE)

    override fun handle(record: OutboxRecord) {
        db.inTx<Unit> { c: Connection ->
            c.prepareStatement("insert into accounts.handle_called (message_id) values (?)").use {
                it.setObject(1, record.messageId())
                it.executeUpdate()
            }
        }
        throw UnsupportedOperationException(
            "Account events require batch delivery"
        )
    }

    override fun handleBatch(
        records: List<OutboxRecord>,
    ): List<Outcome> {
        val failedKeys =
            mutableSetOf<Pair<String, ByteBuffer>>()

        return records.map { record ->
            val key = record.key()?.let {
                record.topic() to ByteBuffer.wrap(it)
            }

            if (
                OutboxHandler.windDownRequested() ||
                key in failedKeys
            ) {
                Outcome.notAttempted()
            } else {
                try {
                    OutboxHandler.inRecordScope(record) {
                        control.apply(ROUTE, record.keyAsString())
                        when (
                            operation.execute(record)
                        ) {
                            AccountEventResult.ACKNOWLEDGE ->
                                Outcome.delivered()

                            AccountEventResult.REQUEUE ->
                                Outcome.requeue()
                        }
                    }
                } catch (failure: Exception) {
                    if (
                        failure is InterruptedException
                    ) {
                        Thread.currentThread().interrupt()
                    }

                    if (key != null) {
                        failedKeys.add(key)
                    }

                    Outcome.retry(failure)
                }
            }
        }
    }
}
