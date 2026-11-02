package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxConfig
import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.OutboxRuntime
import io.github.dsudomoin.outbox.OutboxTransactions
import prodcheck.common.HandlerControl
import ru.tinkoff.kora.application.graph.Lifecycle
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.annotation.Root

/**
 * A second, independent outbox in this process: `OutboxModule` builds one per graph, this one is built here. It depends
 * on the module's `OutboxTransactions`, so the graph releases it before the pools.
 */
@Component
@Root
class SecondOutbox(
    main: OutboxConfig,
    transactions: OutboxTransactions,
    prodcheck: ProdcheckConfig,
    target: TargetHttp,
    control: HandlerControl,
) : Lifecycle {

    companion object {
        const val ROUTE = "second"
    }

    val table: String = prodcheck.second().table()

    val runtime: OutboxRuntime

    init {
        require(table != main.table()) { "prodcheck.second.table must differ from outbox.table, both are $table" }
        // spelled out: Kotlin's `by` does not delegate Java default methods
        val config = object : OutboxConfig {
            override fun table() = this@SecondOutbox.table
            override fun shards() = main.shards()
            override fun relay() = main.relay()
            override fun partitions() = main.partitions()
            override fun inbox() = main.inbox()
            override fun deadLetters() = main.deadLetters()
            override fun recovery() = main.recovery()
            override fun schema() = main.schema()
        }
        val handler = OutboxHandler { record ->
            control.apply(ROUTE, record.keyAsString())
            postOne(target, ROUTE, record)
        }
        runtime = OutboxRuntime.builder(config, transactions, handler).build()
    }

    override fun init() = runtime.start()

    override fun release() = runtime.close()
}
