package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxConfig
import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.OutboxRuntime
import io.github.dsudomoin.outbox.OutboxTransactions
import io.github.dsudomoin.outbox.internal.Tuning
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
    target: TargetHttp,
    control: HandlerControl,
) : Lifecycle {

    companion object {
        const val ROUTE = "second"
    }

    val table = "courier_audit" // the scenarios name it too (s-multi.sh BASES)

    val runtime: OutboxRuntime

    init {
        require(table != main.table()) { "outbox.table must not be the second outbox's $table" }
        // spelled out: Kotlin's `by` does not delegate Java default methods. The main outbox's tuning too, as the
        // scenarios set it for both
        val tuning = Tuning.of(main)
        val config = object : OutboxConfig, Tuning {
            override fun table() = this@SecondOutbox.table
            override fun relay() = main.relay()
            override fun partitions() = main.partitions()
            override fun inbox() = main.inbox()
            override fun deadLetters() = main.deadLetters()
            override fun recovery() = main.recovery()
            override fun schema() = main.schema()
            override fun shards() = tuning.shards()
            override fun networkTimeout() = tuning.networkTimeout()
            override fun maxTicksPerBatch() = tuning.maxTicksPerBatch()
            override fun minTickInterval() = tuning.minTickInterval()
            override fun idlePollInterval() = tuning.idlePollInterval()
            override fun connectionHold() = tuning.connectionHold()
            override fun ticksWithoutRelay() = tuning.ticksWithoutRelay()
            override fun headFailuresBeforeDivert() = tuning.headFailuresBeforeDivert()
            override fun cursorAnchorInterval() = tuning.cursorAnchorInterval()
            override fun requeueDelay() = tuning.requeueDelay()
            override fun waitDelay() = tuning.waitDelay()
            override fun waitMaxDelay() = tuning.waitMaxDelay()
            override fun waitAlertAfter() = tuning.waitAlertAfter()
            override fun xidsPerPartition() = tuning.xidsPerPartition()
            override fun premake() = tuning.premake()
            override fun maintenanceInterval() = tuning.maintenanceInterval()
            override fun swapLockTimeout() = tuning.swapLockTimeout()
            override fun maxTailScan() = tuning.maxTailScan()
            override fun inboxPartitionInterval() = tuning.inboxPartitionInterval()
            override fun inboxPremake() = tuning.inboxPremake()
            override fun deadLetterBatchSize() = tuning.deadLetterBatchSize()
            override fun deadLetterTimeout() = tuning.deadLetterTimeout()
            override fun redriveHistory() = tuning.redriveHistory()
            override fun xidRegression() = tuning.xidRegression()
            override fun recoveryMaxRows() = tuning.recoveryMaxRows()
            override fun recoveryClockMargin() = tuning.recoveryClockMargin()
        }
        runtime = OutboxRuntime.builder(config, transactions, OutboxHandler { postOne(target, control, ROUTE, it) })
            .build()
    }

    override fun init() = runtime.start()

    override fun release() = runtime.close()
}
