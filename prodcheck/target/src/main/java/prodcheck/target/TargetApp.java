package prodcheck.target;

import io.github.dsudomoin.outbox.kora.InboxModule;
import ru.tinkoff.kora.application.graph.KoraApplication;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.config.common.annotation.ConfigSource;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule;
import ru.tinkoff.kora.json.module.JsonModule;
import ru.tinkoff.kora.logging.logback.LogbackModule;
import ru.tinkoff.kora.micrometer.module.MetricsModule;

@KoraApp
public interface TargetApp extends HoconConfigModule, LogbackModule, JdbcDatabaseModule, JsonModule,
        UndertowHttpServerModule, MetricsModule, InboxModule {

    static void main(String[] args) {
        KoraApplication.run(TargetAppGraph::graph);
    }
}
