package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The outbox at a glance ({@link OutboxAdmin#status()}), read from the database at the call, so every instance gives
 * the same answer. {@code halted}: an xid-regression halt is recorded, and no relay delivers; {@code lagSeconds}: from
 * the tick the slowest shard with rows to relay stands at to the latest, 0 when every shard is caught up
 * ({@code outbox.shard.lag}); {@code retrySize}: rows of blocked keys waiting for a retry ({@code outbox.retry.size});
 * {@code parkedSize}: rows in {@code <base>_parked}; {@code waitingSize}: retry heads waiting for an instance that can
 * handle them ({@code outbox.waiting.size}); {@code waitingOldestSeconds}: since the oldest of them began to wait, 0
 * when none waits ({@code outbox.waiting.oldest}); {@code deadLetters}: dead letters by topic and status, null when
 * counting them took longer than the read's 60 s limit (a full scan of the dead table: keep it small with
 * {@code outbox.deadLetters.retention}), the rest of the status read all the same;
 * {@code orphans}: standalone ranges whose rows lie outside the outbox, which the relay cannot deliver;
 * {@code tailOverflow}: rows went to the partition tail ({@code outbox.partitions.overflow}); {@code premade}: ranges
 * ahead of the current xid ({@code outbox.partitions.premade}); {@code xidsLeft}: xids before inserts fail, null while
 * the tail is attached ({@code outbox.partitions.xids_left}); {@code maintenanceSinceSuccessSeconds}: since a
 * maintenance run last completed on any instance, null without that record ({@code outbox.maintenance.since_success});
 * {@code shards}: this instance's shard count, as the bootstrap resolved {@code outbox.shards}; {@code instances}: the
 * instances' heartbeats, by id, expired ones too (a crashed instance's row stays an hour after its heartbeat expired,
 * 3 × {@code maintenanceInterval} after its last beat).
 */
public record OutboxStatus(
        boolean halted,
        double lagSeconds,
        long retrySize,
        long parkedSize,
        long waitingSize,
        double waitingOldestSeconds,
        @Nullable List<DeadLetterCount> deadLetters,
        List<Orphan> orphans,
        boolean tailOverflow,
        int premade,
        @Nullable Long xidsLeft,
        @Nullable Double maintenanceSinceSuccessSeconds,
        int shards,
        List<Instance> instances) {

    public OutboxStatus {
        deadLetters = deadLetters == null ? null : List.copyOf(deadLetters);
        orphans = List.copyOf(orphans);
        instances = List.copyOf(instances);
    }

    /**
     * A running instance, by its heartbeat: {@code id} {@code <host>/<pid>/<8 hex>}, its shard count, when it was last
     * seen (database clock, every {@code maintenanceInterval}), whether it has an active dead-letter hook, whether the
     * relay runs there, {@code live}: its row has not expired, by the database clock, so a deployment gate need not
     * know each instance's interval; {@code contracts}: the highest contract version it reads, by topic, null when
     * unknown, empty when it reports none; a version that is no positive int shows as 0; and {@code routes}: every
     * topic a route of it claims, typed or plain, sorted, null when unknown, empty when it has none. A live instance
     * may still deliver nothing: a heartbeat says the process runs, not that its queue moves
     * ({@code outbox.shard.lag} and {@code outbox.handler.threads.free} do).
     */
    public record Instance(String id, int shards, Instant lastSeen, boolean hookActive, boolean relay, boolean live,
            @Nullable Map<String, Integer> contracts, @Nullable List<String> routes) {

        public Instance {
            contracts = contracts == null ? null : Map.copyOf(contracts);
            routes = routes == null ? null : List.copyOf(routes);
        }
    }

    public record DeadLetterCount(String topic, DeadLetter.Status status, long count) {}

    public enum OrphanKind {
        /**
         * Detached by maintenance as relayed but not dropped; maintenance moves its rows at or above the slowest
         * shard's horizon back itself, unless {@code outbox.partitions.restoreOrphans} is off.
         */
        DETACHED_BY_LIBRARY,
        /**
         * Detached by someone else, perhaps before the relay reached its rows: compare them with what was delivered,
         * then {@link OutboxAdmin#restoreOrphan}.
         */
        FOREIGN,
        /** Set aside by the xid-regression recovery, {@code <base>_o<digits>}: {@link OutboxAdmin#restoreOrphan}. */
        SET_ASIDE
    }

    /** {@code table} as Postgres prints it: unqualified when its schema is on the search_path. */
    public record Orphan(String table, OrphanKind kind) {}
}
