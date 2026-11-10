package prodcheck.store;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import prodcheck.common.Payload;
import prodcheck.common.Sender;

/**
 * shop-n's sender: raw bytes for every topic but {@code orders}, which goes typed as contract 1. Each send also saves
 * the business row through JPA, after the message: outside a transaction the send refuses before anything is written.
 */
@Component
final class StoreSender implements Sender {

    static final int ORDERS_CONTRACT = 1;
    static final List<String> TOPICS = List.of("payments", "ledger", "projection", "orders", "audit");

    private final Outbox outbox;
    private final TypedOutbox<OrderCreatedV1> orders;
    private final StoreOrders rows;
    private final String table;

    StoreSender(Outbox outbox, TypedOutbox<OrderCreatedV1> orders, StoreOrders rows, OutboxConfig config) {
        this.outbox = outbox;
        this.orders = orders;
        this.rows = rows;
        this.table = config.table();
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public List<String> topics() {
        return TOPICS;
    }

    @Override
    public Outbox outbox() {
        return outbox;
    }

    @Override
    public int contract(String topic) {
        return topic.equals("orders") ? ORDERS_CONTRACT : 1;
    }

    @Override
    public UUID send(String topic, @Nullable String key, long seq, String tag) {
        var id = topic.equals("orders") ? orders.send("orders", key, order(seq, tag)) : Sender.super.send(topic, key, seq, tag);
        rows.save(new StoreOrder(id, topic, key, seq, tag));
        return id;
    }

    @Override
    public OutboxMessage message(String topic, @Nullable String key, long seq, String tag) {
        return topic.equals("orders") ? orders.message("orders", key, order(seq, tag)) : Sender.super.message(topic, key, seq, tag);
    }

    private static OrderCreatedV1 order(long seq, String tag) {
        return new OrderCreatedV1(seq, tag, Payload.pad(seq, tag));
    }
}
