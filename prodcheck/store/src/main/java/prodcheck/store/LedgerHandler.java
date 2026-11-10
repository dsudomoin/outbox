package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;
import prodcheck.common.Ledger;

/** shop's database target: its own pool, the arrival in one target transaction, dedupe and effect in another. */
@Component
final class LedgerHandler implements OutboxTopicHandler {

    private final SpringJdbc targetDb;
    private final HandlerControl control;
    private final String instance;

    LedgerHandler(@Qualifier(StoreApp.TARGET_DB) DataSource targetDb, HandlerControl control,
            @Value("${prodcheck.instance}") String instance) {
        this.targetDb = new SpringJdbc(targetDb, new TransactionTemplate(new DataSourceTransactionManager(targetDb)));
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
