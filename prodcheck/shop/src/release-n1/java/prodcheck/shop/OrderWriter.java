package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import prodcheck.common.Payload;
import ru.tinkoff.kora.common.Component;

/** Release N-1 writes v1. */
@Component
public final class OrderWriter {

    static final int CONTRACT = 1;
    static final List<String> TOPICS = List.of("payments", "ledger", "projection", "orders");

    private final TypedOutbox<OrderCreatedV1> events;

    public OrderWriter(TypedOutbox<OrderCreatedV1> events) {
        this.events = events;
    }

    UUID send(@Nullable String key, long seq, String tag) {
        return events.send("orders", key, new OrderCreatedV1(seq, tag, Payload.pad(seq, tag)));
    }

    OutboxMessage message(@Nullable String key, long seq, String tag) {
        return events.message("orders", key, new OrderCreatedV1(seq, tag, Payload.pad(seq, tag)));
    }
}
