package prodcheck.common;

import io.github.dsudomoin.outbox.OutboxMessage;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/** The writer threads behind {@code /load}. */
public final class LoadGenerator {

    public record Settings(int rate, int writers, int keys, List<String> topics, double rollbackPercent,
            Map<String, Integer> tags, int txMessages, long holdMs, boolean bulk, String outbox) {
        Map<String, Object> asMap() {
            var map = new LinkedHashMap<String, Object>();
            map.put("rate", rate);
            map.put("writers", writers);
            map.put("keys", keys);
            map.put("topics", topics);
            map.put("rollbackPercent", rollbackPercent);
            map.put("tags", tags);
            map.put("txMessages", txMessages);
            map.put("holdMs", holdMs);
            map.put("bulk", bulk);
            map.put("outbox", outbox);
            return map;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);

    private static final class RollbackOnPurpose extends RuntimeException {
        RollbackOnPurpose() {
            super(null, null, false, false);
        }
    }

    private record Draft(String topic, String key, String tag) {}

    /** One writer's counts. Topics rotate per tag: with one rotation every 500th message would hit the same topic. */
    private static final class Counts {
        long messages;
        final Map<String, Integer> topicByTag = new HashMap<>();
    }

    private static final Comparator<Draft> LOCK_ORDER = Comparator.comparing(Draft::topic).thenComparing(Draft::key);

    private final JdbcConnectionFactory db;
    private final String app;
    private final String instance;
    private final Map<String, Sender> senders;

    private final LongAdder committed = new LongAdder();
    private final LongAdder rolledBack = new LongAdder();
    private final Map<String, LongAdder> failedByClass = new ConcurrentHashMap<>();

    private volatile boolean stop;
    @Nullable
    private volatile Settings settings;
    private final List<Thread> threads = new ArrayList<>();

    public LoadGenerator(JdbcConnectionFactory db, String app, String instance, Map<String, Sender> senders) {
        this.db = db;
        this.app = app;
        this.instance = instance;
        this.senders = senders;
    }

    public Sender sender(String outbox) {
        var sender = senders.get(outbox);
        if (sender == null) {
            throw new IllegalArgumentException("unknown outbox " + outbox + ", this app has " + senders.keySet());
        }
        return sender;
    }

    public synchronized void start(Settings settings) {
        stop();
        sender(settings.outbox());
        stop = false;
        this.settings = settings;
        for (var i = 0; i < settings.writers(); i++) {
            var thread = new Thread(() -> write(settings), "load-writer-" + i);
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
        }
    }

    /** Returns when every writer has left its transaction. */
    public synchronized void stop() {
        stop = true;
        var interrupted = false;
        for (var thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        threads.clear();
        settings = null;
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<String, Object> stats() {
        var map = new LinkedHashMap<String, Object>();
        map.put("committed", committed.sum());
        map.put("rolledBack", rolledBack.sum());
        var failed = new TreeMap<String, Long>();
        failedByClass.forEach((k, v) -> failed.put(k, v.sum()));
        map.put("failedByClass", failed);
        var current = settings;
        map.put("load", current == null ? null : current.asMap());
        return map;
    }

    private void write(Settings s) {
        var sender = sender(s.outbox());
        var perTxNanos = s.rate() > 0 ? (long) (s.txMessages() * s.writers() * 1e9 / s.rate()) : 0;
        var next = System.nanoTime();
        var counts = new Counts();
        while (!stop) {
            try {
                if (perTxNanos > 0) {
                    next = pace(next, perTxNanos);
                }
                if (!stop) {
                    if (s.bulk()) {
                        bulkTx(s, sender, counts);
                    } else {
                        tx(s, sender, counts);
                    }
                }
            } catch (Throwable t) {
                failedByClass.computeIfAbsent(rootCause(t).getClass().getName(), k -> new LongAdder()).increment();
                log.debug("writer transaction failed", t);
                LockSupport.parkNanos(100_000_000L);
            }
        }
    }

    private static long pace(long next, long perTxNanos) {
        next += perTxNanos;
        var late = System.nanoTime() - next;
        if (late > 1_000_000_000L) { // behind after a stall: no burst to catch up
            return System.nanoTime();
        }
        if (late < 0) {
            LockSupport.parkNanos(-late);
        }
        return next;
    }

    private void tx(Settings s, Sender sender, Counts counts) {
        var random = ThreadLocalRandom.current();
        var drafts = new ArrayList<Draft>(s.txMessages());
        for (var i = 0; i < s.txMessages(); i++) {
            var tag = tag(s, counts);
            drafts.add(new Draft(topic(s, sender, counts, tag), "k" + random.nextInt(s.keys()), tag));
        }
        drafts.sort(LOCK_ORDER);
        var rollback = random.nextDouble() * 100 < s.rollbackPercent();
        var rows = new ArrayList<Ledger.Sent>(drafts.size());
        try {
            db.inTx(c -> {
                for (var d : drafts) {
                    var seq = Ledger.nextSeq(c, d.topic(), d.key());
                    var id = sender.send(d.topic(), d.key(), seq, d.tag());
                    rows.add(row(sender, id, d, seq));
                }
                Ledger.sent(c, rows);
                finish(s, rollback);
            });
            committed.add(rows.size());
        } catch (RollbackOnPurpose e) {
            noteRollback(rows);
        }
    }

    private void bulkTx(Settings s, Sender sender, Counts counts) {
        var drafts = new ArrayList<Draft>(s.txMessages());
        var messages = new ArrayList<OutboxMessage>(s.txMessages());
        for (var i = 0; i < s.txMessages(); i++) {
            var tag = tag(s, counts);
            var d = new Draft(topic(s, sender, counts, tag), UUID.randomUUID().toString(), tag);
            drafts.add(d);
            messages.add(sender.message(d.topic(), d.key(), 0, d.tag()));
        }
        var rollback = ThreadLocalRandom.current().nextDouble() * 100 < s.rollbackPercent();
        var rows = new ArrayList<Ledger.Sent>(drafts.size());
        try {
            db.inTx(c -> {
                var ids = sender.outbox().sendAll(messages);
                for (var i = 0; i < ids.size(); i++) {
                    rows.add(row(sender, ids.get(i), drafts.get(i), 0));
                }
                Ledger.sent(c, rows);
                finish(s, rollback);
            });
            committed.add(rows.size());
        } catch (RollbackOnPurpose e) {
            noteRollback(rows);
        }
    }

    private Ledger.Sent row(Sender sender, UUID id, Draft d, long seq) {
        return new Ledger.Sent(id, app, instance, sender.table(), d.topic(), d.key(), seq, d.tag(),
                sender.contract(d.topic()));
    }

    /** Holds the transaction, then rolls it back if it should: last, so the rollback undoes real work. */
    private void finish(Settings s, boolean rollback) {
        var until = System.nanoTime() + s.holdMs() * 1_000_000L;
        while (s.holdMs() > 0 && !stop && System.nanoTime() < until) {
            LockSupport.parkNanos(Math.min(until - System.nanoTime(), 50_000_000L));
        }
        if (rollback) {
            throw new RollbackOnPurpose();
        }
    }

    private void noteRollback(List<Ledger.Sent> rows) {
        db.inTx(c -> {
            Ledger.rolledBack(c, rows);
        });
        rolledBack.add(rows.size());
    }

    private static String topic(Settings s, Sender sender, Counts counts, String tag) {
        var topics = s.topics() != null ? s.topics() : sender.topics();
        var turn = counts.topicByTag.merge(tag, 1, (n, one) -> (n + one) % topics.size());
        return topics.get(turn % topics.size());
    }

    private static String tag(Settings s, Counts counts) {
        var n = ++counts.messages;
        for (var tag : s.tags().entrySet()) {
            if (n % tag.getValue() == 0) {
                return tag.getKey();
            }
        }
        return "normal";
    }

    static Throwable rootCause(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t;
    }
}
