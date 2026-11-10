package prodcheck.store;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;
import prodcheck.common.Ledger;

/**
 * The Spring case of shop's projection: a {@code @Transactional} handler with Inbox on the application's own pool.
 * The arrival commits on its own ({@code REQUIRES_NEW}), the inbox marker and the effect in the handler's transaction.
 */
@Component
class ProjectionHandler implements OutboxTopicHandler {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate arrival;
    private final Inbox inbox;
    private final HandlerControl control;
    private final String instance;

    ProjectionHandler(JdbcTemplate jdbc, PlatformTransactionManager transactions, Inbox inbox, HandlerControl control,
            @Value("${prodcheck.instance}") String instance) {
        this.jdbc = jdbc;
        this.arrival = new TransactionTemplate(transactions);
        this.arrival.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.inbox = inbox.scope("projection");
        this.control = control;
        this.instance = instance;
    }

    @Override
    public Set<String> topics() {
        return Set.of("projection");
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void handle(OutboxRecord record) throws Exception {
        var event = Event.of(record, "projection", instance);
        arrival.executeWithoutResult(status -> jdbc.execute((ConnectionCallback<Object>) c -> {
            Ledger.arrival(c, event);
            return null;
        }));
        control.applyInProcess("projection", event);
        if (inbox.tryAccept(event.id())) {
            jdbc.execute((ConnectionCallback<Object>) c -> {
                Ledger.effect(c, event);
                return null;
            });
        }
    }
}
