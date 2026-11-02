package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import prodcheck.common.Payload;
import ru.tinkoff.kora.common.Component;

/** Release N+1 writes v2. */
@Component
public final class OrderWriter {

    static final int CONTRACT = 2;
    static final List<String> TOPICS = List.of("payments", "ledger", "projection", "orders", "audit");

    private final TypedOutbox<OrderCreated> events;

    public OrderWriter(TypedOutbox<OrderCreated> events) {
        this.events = events;
    }

    UUID send(@Nullable String key, long seq, String tag) {
        return events.send("orders", key, new OrderCreated(seq, tag, Payload.pad(seq, tag), "RUB"));
    }

    OutboxMessage message(@Nullable String key, long seq, String tag) {
        return events.message("orders", key, new OrderCreated(seq, tag, Payload.pad(seq, tag), "RUB"));
    }
}
