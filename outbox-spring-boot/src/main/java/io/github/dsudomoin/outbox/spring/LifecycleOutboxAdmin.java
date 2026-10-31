package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.DeadLetterPage;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxStatus;
import jakarta.annotation.Nullable;
import java.util.Collection;

/** The {@code OutboxAdmin} bean: the admin of the lifecycle's latest start. */
final class LifecycleOutboxAdmin implements OutboxAdmin {

    private final OutboxLifecycle lifecycle;

    LifecycleOutboxAdmin(OutboxLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    private OutboxAdmin admin() {
        return lifecycle.current().admin();
    }

    @Override
    public OutboxStatus status() {
        return admin().status();
    }

    @Override
    public DeadLetterPage findDeadLetters(DeadLetterFilter filter, @Nullable Long afterId, int limit) {
        return admin().findDeadLetters(filter, afterId, limit);
    }

    @Override
    public long countDeadLetters(DeadLetterFilter filter) {
        return admin().countDeadLetters(filter);
    }

    @Override
    public long redriveDeadLetters(Collection<Long> ids) {
        return admin().redriveDeadLetters(ids);
    }

    @Override
    public long redriveDeadLetters(DeadLetterFilter filter) {
        return admin().redriveDeadLetters(filter);
    }

    @Override
    public long discardDeadLetters(Collection<Long> ids) {
        return admin().discardDeadLetters(ids);
    }

    @Override
    public long discardDeadLetters(DeadLetterFilter filter) {
        return admin().discardDeadLetters(filter);
    }

    @Override
    public long restoreOrphan(String table, long fromXid) {
        return admin().restoreOrphan(table, fromXid);
    }

    @Override
    public long recoverXidRegression() {
        return admin().recoverXidRegression();
    }

    @Override
    public Abandoned abandonWaiting(String topic, @Nullable byte[] key, String reason, boolean dryRun) {
        return admin().abandonWaiting(topic, key, reason, dryRun);
    }
}
