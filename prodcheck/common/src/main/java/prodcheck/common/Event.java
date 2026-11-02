package prodcheck.common;

import io.github.dsudomoin.outbox.OutboxRecord;
import jakarta.annotation.Nullable;
import java.util.UUID;

/** One message as a sink sees it. */
public record Event(String sink, UUID id, String topic, @Nullable String key, long seq, String tag, String instance,
        int contract) {

    public static Event of(OutboxRecord record, String sink, String instance) {
        return new Event(sink, record.messageId(), record.topic(), record.keyAsString(),
                Payload.seq(record.payload()), Payload.tag(record.payload()), instance, record.contractVersion());
    }
}
