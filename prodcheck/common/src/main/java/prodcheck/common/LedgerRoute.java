package prodcheck.common;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/** A database target: its own pool, the arrival in one target transaction, the dedupe and the effect in another. */
public final class LedgerRoute implements OutboxTopicHandler {

    private final JdbcConnectionFactory targetDb;
    private final HandlerControl control;
    private final String instance;

    public LedgerRoute(JdbcConnectionFactory targetDb, HandlerControl control, String instance) {
        this.targetDb = targetDb;
        this.control = control;
        this.instance = instance;
    }

    @Override
    public Set<String> topics() {
        return Set.of("ledger");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        var event = Event.of(record, "ledger", instance);
        targetDb.inTx(c -> {
            Ledger.arrival(c, event);
        });
        control.applyInProcess("ledger", event);
        targetDb.inTx(c -> {
            if (Ledger.dedupe(c, event.id())) {
                Ledger.effect(c, event);
            }
        });
    }
}
