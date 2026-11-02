package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.util.Set;
import ru.tinkoff.kora.common.Component;

/** Reads v1 and v2. */
@Component
public final class OrdersHandler extends TypedOutboxTopicHandler<OrderCreated> {

    private final TargetRoute target;

    public OrdersHandler(OutboxDeserializer<OrderCreated> deserializer, TargetRoute target) {
        super(Set.of("orders"), deserializer);
        this.target = target;
    }

    @Override
    protected void handle(OrderCreated value, OutboxRecord record) throws Exception {
        target.post("orders", record);
    }
}
