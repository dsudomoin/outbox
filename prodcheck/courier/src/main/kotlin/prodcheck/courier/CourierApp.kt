package prodcheck.courier

import io.github.dsudomoin.outbox.Outbox
import io.github.dsudomoin.outbox.OutboxConfig
import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminHttpModule
import io.github.dsudomoin.outbox.kora.OutboxModule
import prodcheck.common.ApiKeyAuthorizer
import prodcheck.common.Controls
import prodcheck.common.HandlerControl
import prodcheck.common.Sender
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.common.Config
import ru.tinkoff.kora.config.common.annotation.ConfigSource
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule
import ru.tinkoff.kora.json.module.JsonModule
import ru.tinkoff.kora.logging.logback.LogbackModule
import ru.tinkoff.kora.micrometer.module.MetricsModule
import java.time.Duration

@KoraApp
interface CourierApp :
    HoconConfigModule,
    LogbackModule,
    JdbcDatabaseModule,
    JsonModule,
    UndertowHttpServerModule,
    MetricsModule,
    OutboxModule,
    OutboxAdminHttpModule {

    /** Exactly `OutboxHandler`: the one handler behind the routes. */
    fun outboxHandler(target: TargetHttp, control: HandlerControl): OutboxHandler = CourierHandler(target, control)

    fun outboxAdminAuthorizer(config: Config): OutboxAdminAuthorizer = ApiKeyAuthorizer(
        config.get("outboxAdmin.adminKey").asString(),
        config.get("outboxAdmin.readerKey").asString(),
    )

    fun handlerControl(): HandlerControl = HandlerControl(
        listOf(CourierHandler.ROUTE, RoutedHandler.ROUTE, SecondOutbox.ROUTE),
        setOf("throw", "error", "null-list", "wrong-size", "null-element"),
    )

    fun controls(
        db: JdbcConnectionFactory,
        outbox: Outbox,
        outboxConfig: OutboxConfig,
        second: SecondOutbox,
        handlers: HandlerControl,
        config: ProdcheckConfig,
    ): Controls = Controls(
        db,
        "courier",
        config.instance(),
        mapOf(
            "main" to Sender.raw(outbox, outboxConfig.table(), listOf("deliveries", "receipts", "routed")),
            "second" to Sender.raw(second.runtime.outbox(), second.table, listOf("second")),
        ),
        handlers,
    )
}

@ConfigSource("prodcheck")
interface ProdcheckConfig {
    fun instance(): String
    fun second(): Second

    @ConfigValueExtractor
    interface Second {
        fun table(): String
    }
}

@ConfigSource("httpClient.target")
interface TargetHttpConfig {
    fun url(): String
    fun requestTimeout(): Duration
}

fun main() {
    KoraApplication.run { CourierAppGraph.graph() }
}
