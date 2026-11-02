package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.Set;
import ru.tinkoff.kora.common.Component;

@Component
public final class PaymentsHandler implements OutboxTopicHandler {

    private final TargetRoute target;

    public PaymentsHandler(TargetRoute target) {
        this.target = target;
    }

    @Override
    public Set<String> topics() {
        return Set.of("payments");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        target.post("payments", record);
    }
}
