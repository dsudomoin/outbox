package prodcheck.shop;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer;
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminHttpModule;
import io.github.dsudomoin.outbox.kora.InboxModule;
import io.github.dsudomoin.outbox.kora.OutboxJsonModule;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import java.util.Map;
import java.util.Set;
import prodcheck.common.ApiKeyAuthorizer;
import prodcheck.common.Controls;
import prodcheck.common.HandlerControl;
import ru.tinkoff.kora.application.graph.KoraApplication;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.config.common.Config;
import ru.tinkoff.kora.config.common.annotation.ConfigSource;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.http.client.ok.OkHttpClientModule;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule;
import ru.tinkoff.kora.json.module.JsonModule;
import ru.tinkoff.kora.logging.logback.LogbackModule;
import ru.tinkoff.kora.micrometer.module.MetricsModule;

@KoraApp
public interface ShopApp extends HoconConfigModule, LogbackModule, JdbcDatabaseModule, JsonModule,
        UndertowHttpServerModule, MetricsModule, OkHttpClientModule, OutboxModule, OutboxJsonModule, InboxModule,
        OutboxAdminHttpModule {

    /** Tag of the pool on the target database, for the {@code ledger} route. */
    final class TargetDb {}

    @ConfigSource("prodcheck")
    interface ProdcheckConfig {
        String instance();

        String hook();
    }

    static void main(String[] args) {
        KoraApplication.run(ShopAppGraph::graph);
    }

    @Tag(TargetDb.class)
    default JdbcDatabase targetDatabase(Config config, ConfigValueExtractor<JdbcDatabaseConfig> extractor,
            DataBaseTelemetryFactory telemetry) {
        return new JdbcDatabase(extractor.extract(config.get("targetDb")), telemetry);
    }

    default OutboxAdminAuthorizer outboxAdminAuthorizer(Config config) {
        return new ApiKeyAuthorizer(config.get("outboxAdmin.adminKey").asString(),
                config.get("outboxAdmin.readerKey").asString());
    }

    default DeadLetterPolicy deadLetterPolicy(ProdcheckConfig config) {
        return new DeadLetterPolicy(config.hook().equals("policy"));
    }

    default HandlerControl handlerControl() {
        return new HandlerControl(OrderWriter.TOPICS, Set.of());
    }

    default Controls controls(JdbcConnectionFactory db, Outbox outbox, OutboxConfig outboxConfig, OrderWriter orders,
            HandlerControl handlers, ProdcheckConfig config) {
        var sender = new ShopSender(outbox, outboxConfig.table(), orders);
        return new Controls(db, "shop", config.instance(), Map.of("main", sender), handlers);
    }
}
