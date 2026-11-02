package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.util.Set;
import ru.tinkoff.kora.common.Component;

/** Release N-1 reads only v1. */
@Component
public final class OrdersHandler extends TypedOutboxTopicHandler<OrderCreatedV1> {

    private final TargetRoute target;

    public OrdersHandler(OutboxDeserializer<OrderCreatedV1> deserializer, TargetRoute target) {
        super(Set.of("orders"), deserializer);
        this.target = target;
    }

    @Override
    protected void handle(OrderCreatedV1 value, OutboxRecord record) throws Exception {
        target.post("orders", record);
    }
}
