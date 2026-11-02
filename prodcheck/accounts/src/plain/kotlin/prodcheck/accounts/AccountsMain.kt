package prodcheck.accounts

import io.github.dsudomoin.outbox.kora.OutboxProtobufModule
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule
import ru.tinkoff.kora.logging.logback.LogbackModule

@KoraApp
interface AccountsApp :
    HoconConfigModule,
    LogbackModule,
    JdbcDatabaseModule,
    UndertowHttpServerModule,
    AccountsOutboxModule,
    OutboxProtobufModule,
    AccountsControlsModule

fun main() {
    KoraApplication.run { AccountsAppGraph.graph() }
}
