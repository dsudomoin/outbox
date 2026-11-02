package prodcheck.accounts

import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminHttpModule
import io.github.dsudomoin.outbox.kora.OutboxProtobufModule
import prodcheck.common.ApiKeyAuthorizer
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.common.Config
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule
import ru.tinkoff.kora.json.module.JsonModule
import ru.tinkoff.kora.logging.logback.LogbackModule
import ru.tinkoff.kora.micrometer.module.MetricsModule

@KoraApp
interface AccountsApp :
    HoconConfigModule,
    LogbackModule,
    JdbcDatabaseModule,
    JsonModule,
    UndertowHttpServerModule,
    MetricsModule,
    AccountsOutboxModule,
    OutboxProtobufModule,
    OutboxAdminHttpModule,
    AccountsControlsModule {

    fun outboxAdminAuthorizer(config: Config): OutboxAdminAuthorizer = ApiKeyAuthorizer(
        config.get("outboxAdmin.adminKey").asString(),
        config.get("outboxAdmin.readerKey").asString(),
    )
}

fun main() {
    KoraApplication.run { AccountsAppGraph.graph() }
}
