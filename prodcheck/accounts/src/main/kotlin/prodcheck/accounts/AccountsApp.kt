package prodcheck.accounts

import io.github.dsudomoin.outbox.Outbox
import io.github.dsudomoin.outbox.OutboxConfig
import io.github.dsudomoin.outbox.kora.OutboxModule
import io.github.dsudomoin.outbox.kora.OutboxOwner
import io.github.dsudomoin.outbox.kora.OutboxProtobufModule
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox
import prodcheck.common.Controls
import prodcheck.common.HandlerControl
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.common.Tag
import ru.tinkoff.kora.config.common.Config
import ru.tinkoff.kora.config.common.annotation.ConfigSource
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcDatabase
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule
import ru.tinkoff.kora.logging.logback.LogbackModule

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

@KoraApp
interface AccountsApp :
    HoconConfigModule,
    LogbackModule,
    JdbcDatabaseModule,
    UndertowHttpServerModule,
    AccountsOutboxModule,
    OutboxProtobufModule {

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
}

fun main() {
    KoraApplication.run { AccountsAppGraph.graph() }
}
