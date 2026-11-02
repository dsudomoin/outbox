package prodcheck.shop;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import prodcheck.common.Sender;

/** Raw bytes for every topic but {@code orders}, which goes through the release's TypedOutbox. */
final class ShopSender implements Sender {

    private final Outbox outbox;
    private final String table;
    private final OrderWriter orders;

    ShopSender(Outbox outbox, String table, OrderWriter orders) {
        this.outbox = outbox;
        this.table = table;
        this.orders = orders;
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public List<String> topics() {
        return OrderWriter.TOPICS;
    }

    @Override
    public Outbox outbox() {
        return outbox;
    }

    @Override
    public int contract(String topic) {
        return topic.equals("orders") ? OrderWriter.CONTRACT : 1;
    }

    @Override
    public UUID send(String topic, @Nullable String key, long seq, String tag) {
        return topic.equals("orders") ? orders.send(key, seq, tag) : Sender.super.send(topic, key, seq, tag);
    }

    @Override
    public OutboxMessage message(String topic, @Nullable String key, long seq, String tag) {
        return topic.equals("orders") ? orders.message(key, seq, tag) : Sender.super.message(topic, key, seq, tag);
    }
}
