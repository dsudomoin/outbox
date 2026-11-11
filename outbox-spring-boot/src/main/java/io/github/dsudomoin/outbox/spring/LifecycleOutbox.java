package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.internal.Contracts;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;

/** The {@code Outbox} bean: the outbox of the lifecycle's latest start. */
final class LifecycleOutbox implements Outbox, Contracts.TypedSend {

    private final OutboxLifecycle lifecycle;

    LifecycleOutbox(OutboxLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public UUID send(OutboxMessage message) {
        return lifecycle.current().outbox().send(message);
    }

    @Override
    public UUID send(OutboxMessage message, String type) {
        return Contracts.send(lifecycle.current().outbox(), message, type);
    }

    @Override
    public UUID send(Connection connection, OutboxMessage message) {
        return lifecycle.current().outbox().send(connection, message);
    }

    @Override
    public List<UUID> sendAll(List<OutboxMessage> messages) {
        return lifecycle.current().outbox().sendAll(messages);
    }
}
