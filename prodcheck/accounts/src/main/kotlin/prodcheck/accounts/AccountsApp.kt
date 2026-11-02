package prodcheck.accounts

import io.github.dsudomoin.outbox.Outbox
import io.github.dsudomoin.outbox.OutboxConfig
import io.github.dsudomoin.outbox.kora.OutboxModule
import io.github.dsudomoin.outbox.kora.OutboxOwner
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox
import prodcheck.common.Controls
import prodcheck.common.HandlerControl
import ru.tinkoff.kora.common.Tag
import ru.tinkoff.kora.config.common.Config
import ru.tinkoff.kora.config.common.annotation.ConfigSource
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcDatabase
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig

interface AccountsOutboxModule : OutboxModule {
    @Tag(OutboxOwner::class)
    fun ownerDatabaseConfig(
        config: Config,
        extractor: ConfigValueExtractor<JdbcDatabaseConfig>,
    ): JdbcDatabaseConfig =
        extractor.extract(config.get("ownerDb"))

    @Tag(OutboxOwner::class)
    fun ownerDatabase(
        @Tag(OutboxOwner::class) config: JdbcDatabaseConfig,
        telemetryFactory: DataBaseTelemetryFactory,
    ): JdbcDatabase =
        JdbcDatabase(config, telemetryFactory, null)
}

/** What both builds of the app have: its plain graph mirrors the service, the full one adds metrics and the admin API. */
interface AccountsControlsModule {
    fun handlerControl(): HandlerControl = HandlerControl(listOf(ROUTE), setOf())

    fun controls(
        db: JdbcConnectionFactory,
        outbox: Outbox,
        producer: ProtobufOutbox,
        outboxConfig: OutboxConfig,
        handlers: HandlerControl,
        config: ProdcheckConfig,
    ): Controls = Controls(
        db,
        "accounts",
        config.instance(),
        mapOf("main" to AccountsSender(outbox, producer, outboxConfig.table())),
        handlers,
    )
}

const val ROUTE = "account-events"

@ConfigSource("prodcheck")
interface ProdcheckConfig {
    fun instance(): String
}

@ConfigSource("accounts")
interface AccountsConfig {
    fun lockTimeoutMs(): Long

    fun holdSameType(): Boolean

    /** A source this release does not know: `ack` (the service: gone, no effect) or `wait` (for a release that reads it). */
    fun unknownSource(): String

    /** The source numbers this release reads, comma separated; absent in an envelope is 0. */
    fun knownSources(): String

    /** An envelope that does not parse: false, Retry for ever (the service); true, Outcome.permanent, the key moves on. */
    fun corruptPermanent(): Boolean
}

