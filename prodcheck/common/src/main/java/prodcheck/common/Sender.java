package prodcheck.common;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/** One outbox of the app as the writers see it. An app overrides what a typed topic does differently. */
public interface Sender {

    /** {@code pc_sent.outbox}. */
    String table();

    /** What {@code /load} writes without {@code topics=}. */
    List<String> topics();

    Outbox outbox();

    default int contract(String topic) {
        return 1;
    }

    default UUID send(String topic, @Nullable String key, long seq, String tag) {
        return outbox().send(message(topic, key, seq, tag));
    }

    default OutboxMessage message(String topic, @Nullable String key, long seq, String tag) {
        var payload = Payload.of(seq, tag);
        return key == null ? OutboxMessage.of(topic, payload) : OutboxMessage.of(topic, key, payload);
    }

    static Sender raw(Outbox outbox, String table, List<String> topics) {
        return new Sender() {
            @Override
            public String table() {
                return table;
            }

            @Override
            public List<String> topics() {
                return topics;
            }

            @Override
            public Outbox outbox() {
                return outbox;
            }
        };
    }
}
