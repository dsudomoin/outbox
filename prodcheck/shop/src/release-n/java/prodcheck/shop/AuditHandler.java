package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import ru.tinkoff.kora.common.Component;

/** The route release N adds. */
@Component
public final class AuditHandler implements OutboxTopicHandler {

    private final TargetRoute target;

    public AuditHandler(TargetRoute target) {
        this.target = target;
    }

    @Override
    public Set<String> topics() {
        return Set.of("audit");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        target.post("audit", record);
    }
}
