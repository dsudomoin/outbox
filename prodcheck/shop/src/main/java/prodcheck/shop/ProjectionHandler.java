package prodcheck.shop;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;
import prodcheck.common.Ledger;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/** Inbox in the source database, on the application's own pool: on purpose, it competes with the writers. */
@Component
public final class ProjectionHandler implements OutboxTopicHandler {

    private final JdbcConnectionFactory db;
    private final Inbox inbox;
    private final HandlerControl control;
    private final String instance;

    public ProjectionHandler(JdbcConnectionFactory db, Inbox inbox, HandlerControl control,
            ShopApp.ProdcheckConfig config) {
        this.db = db;
        this.inbox = inbox.scope("projection");
        this.control = control;
        this.instance = config.instance();
    }

    @Override
    public Set<String> topics() {
        return Set.of("projection");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        var event = Event.of(record, "projection", instance);
        db.inTx(c -> {
            Ledger.arrival(c, event);
        });
        control.applyInProcess("projection", event);
        db.inTx(c -> {
            if (inbox.tryAccept(event.id())) {
                Ledger.effect(c, event);
            }
        });
    }
}
