package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;

/**
 * How a record fails that this instance cannot handle: no route claims its topic and there is no handler behind the
 * routes, its contract version is newer than its typed handler reads, its payload does not decode, or it is a tombstone
 * its typed handler does not take. The first two wait for an instance that can handle the record
 * ({@link OutboxWaitException}), the others can never succeed, built here; a deserializer may throw
 * {@link OutboxWaitException} or {@link OutboxPermanentException} itself.
 */
final class CannotHandle {

    private CannotHandle() {}

    /** {@code Permanent}: no instance reading {@code record}'s contract version can ever handle it. */
    static OutboxPermanentException permanent(OutboxRecord record, String reason, @Nullable Throwable cause) {
        return new OutboxPermanentException("cannot handle outbox record " + record.messageId() + " of topic "
                + record.topic() + ": " + reason, cause);
    }
}
