package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;
import prodcheck.common.Ledger;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/** A database target: its own pool, the dedupe and the effect in one target transaction. */
@Component
public final class LedgerHandler implements OutboxTopicHandler {

    private final JdbcDatabase targetDb;
    private final HandlerControl control;
    private final String instance;

    public LedgerHandler(@Tag(ShopApp.TargetDb.class) JdbcDatabase targetDb, HandlerControl control,
            ShopApp.ProdcheckConfig config) {
        this.targetDb = targetDb;
        this.control = control;
        this.instance = config.instance();
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
