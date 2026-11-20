package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The observation of one message a {@code send} or {@code sendAll} call writes. Its carrier collects the trace headers
 * the application's propagating sender handler injects; the message stores them unless it carries its own.
 */
public final class OutboxEnqueueContext extends SenderContext<List<OutboxHeader>> {

    private final OutboxMessage message;
    private final UUID messageId;
    private final int shard;
    /** The default convention's high cardinality values, once computed; the writer's thread starts and stops it. */
    @Nullable
    KeyValues highCardinality;

    OutboxEnqueueContext(OutboxMessage message, UUID messageId, int shard) {
        super((carrier, key, value) -> {
            if (carrier != null) {
                carrier.add(new OutboxHeader(key, value.getBytes(UTF_8)));
            }
        }, Kind.PRODUCER);
        this.message = message;
        this.messageId = messageId;
        this.shard = shard;
        setCarrier(new ArrayList<>(2));
    }

    /** The message as the caller passed it. */
    public OutboxMessage message() {
        return message;
    }

    /** The id the message is stored with, {@code OutboxRecord.messageId()}. */
    public UUID messageId() {
        return messageId;
    }

    /** The shard the message is stored on, {@code OutboxRecord.shard()}. */
    public int shard() {
        return shard;
    }
}
