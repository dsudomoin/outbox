package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.kafka.KafkaCluster.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxStatus;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Broker faults under load against a real three-node cluster ({@link KafkaCluster}): a fleet of two
 * {@link OutboxRuntime}s on the same tables, each with its own {@link KafkaOutboxSender}, while writers commit
 * continuously to a topic of 6 partitions, replication factor 3, {@code min.insync.replicas=2}. After each fault heals
 * and the writers stop, the backlog must drain within {@link #DRAIN}, and a consumer reading the topic from the start
 * must find every committed message, per key in send order, with nothing left in the retry, parked or dead tables.
 * Each case prints {@code <case> sent= lost= duplicates= maxLatencyMs=} (latency: the broker's append time of the
 * first copy less the writer's commit time) and a details line.
 */
@Tag("chaos")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
class KafkaClusterFaultsIT {

    static {
        // the clients' reconnect WARNs are the evidence; their INFO (configs, metadata) drowns it
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");
    }

    private static final int PARTITIONS = 6;
    private static final int WRITERS = 4;
    private static final int KEYS_PER_WRITER = 16;
    private static final int MESSAGES_PER_TX = 8;
    private static final Duration WRITE_PAUSE = Duration.ofMillis(40);
    private static final Duration WARMUP = Duration.ofSeconds(5);
    private static final Duration AFTER_HEAL = Duration.ofSeconds(5);
    /** From the writers' stop until every committed message is in the topic: longer is a stall. */
    private static final Duration DRAIN = Duration.ofSeconds(90);

    private static KafkaCluster cluster;

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> closing = new ArrayList<>(); // senders and runtimes, closed last first
    /** Per key, the ids of the committed messages in send order; written by the key's one writer. */
    private final Map<String, List<UUID>> committed = new ConcurrentHashMap<>();
    private final Map<UUID, Long> committedAtMs = new ConcurrentHashMap<>();
    /** Per delivered id, when the first handler call that reported it delivered began (nanoTime). */
    private final Map<UUID, Long> deliveredInCallFrom = new ConcurrentHashMap<>();
    /** What the handlers answered other than Delivered, by outcome and error, digits and the topic name masked. */
    private final Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();
    /** Failed outcomes whose cause chain holds NotEnoughReplicas(AfterAppend)Exception. */
    private final AtomicInteger notEnoughReplicasOutcomes = new AtomicInteger();
    private volatile Supplier<String> extraDetails = () -> "";
    private final AtomicBoolean writing = new AtomicBoolean();
    private final List<Future<?>> writers = new ArrayList<>();
    private ExecutorService pool;
    private String topic;
    private List<OutboxRuntime> runtimes;
    private OutboxStatus duringFault;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = new KafkaCluster();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @AfterEach
    void close() throws Exception {
        writing.set(false);
        if (pool != null) {
            pool.shutdownNow();
        }
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        cluster.heal();
    }

    /** (a) kill -9 of the node leading the most partitions, started again 15 s later. */
    @Test
    void theLeaderOfMostPartitionsIsKilled() {
        start();
        int leader = cluster.leaderOfMostPartitions(topic);
        System.out.println("a: killing node " + leader + ", leaders " + cluster.leaders(topic));
        cluster.kill(leader);
        hold(Duration.ofSeconds(15));
        cluster.start(leader);
        finish("a-kill-leader", Long.MIN_VALUE);
    }

    /** (b) two of three nodes killed for 20 s: below min.insync.replicas, and the controller quorum is lost too. */
    @Test
    void twoNodesAreDownAtOnce() {
        start();
        int leader = cluster.leaderOfMostPartitions(topic);
        int other = leader % 3 + 1;
        System.out.println("b: killing nodes " + leader + " and " + other + ", leaders " + cluster.leaders(topic));
        cluster.kill(leader);
        cluster.kill(other);
        hold(Duration.ofSeconds(20));
        cluster.start(leader);
        cluster.start(other);
        finish("b-two-down", Long.MIN_VALUE);
    }

    /**
     * (b2) the quorum alive, the brokers below {@code min.insync.replicas}: the topic raised to 3, one node killed for
     * 20 s, so every partition keeps 2 in sync and its leader answers {@code NOT_ENOUGH_REPLICAS} to acks=all. Kafka's
     * producer retries that itself, so the count comes from its retry WARNs and from the relay's outcomes whose cause
     * chain holds the exception.
     */
    @Test
    void oneNodeDownBelowMinInsyncReplicas() {
        start();
        cluster.setTopicConfig(topic, "min.insync.replicas", "3");
        sleep(Duration.ofSeconds(2)); // the leaders apply it from the metadata log
        int leader = cluster.leaderOfMostPartitions(topic);
        System.out.println("b2: min.insync.replicas=3, killing node " + leader + ", leaders " + cluster.leaders(topic));
        try (var err = new CapturedStderr()) {
            cluster.kill(leader);
            hold(Duration.ofSeconds(20));
            cluster.start(leader);
            extraDetails = () -> " notEnoughReplicasLogged=" + Pattern.compile("NOT_ENOUGH_REPLICAS").matcher(err.text())
                    .results().count() + " notEnoughReplicasOutcomes=" + notEnoughReplicasOutcomes.get();
            finish("b2-below-min-isr", Long.MIN_VALUE);
            assertThat(Pattern.compile("NOT_ENOUGH_REPLICAS").matcher(err.text()).results().count()
                    + notEnoughReplicasOutcomes.get()).as("NotEnoughReplicas seen by the producer").isPositive();
        }
    }

    /** (c) the node leading the most partitions frozen (docker pause) for 25 s. */
    @Test
    void theLeaderOfMostPartitionsFreezes() {
        start();
        int leader = cluster.leaderOfMostPartitions(topic);
        System.out.println("c: pausing node " + leader + ", leaders " + cluster.leaders(topic));
        cluster.pause(leader);
        hold(Duration.ofSeconds(25));
        cluster.unpause(leader);
        finish("c-frozen-leader", Long.MIN_VALUE);
    }

    /** (d) every node unreachable from the clients for 25 s, the brokers still in touch with each other. */
    @Test
    void theWholeClusterIsUnreachable() {
        start();
        IntStream.rangeClosed(1, 3).forEach(n -> cluster.blackhole(n, true));
        hold(Duration.ofSeconds(25));
        IntStream.rangeClosed(1, 3).forEach(n -> cluster.blackhole(n, false));
        finish("d-blackhole-all", Long.MIN_VALUE);
    }

    /**
     * (e) the node leading the most partitions slow for 25 s: its client traffic delayed by 500 ms per chunk each way,
     * which also caps what it takes per second below what the writers send it. Replication between the brokers is not
     * slowed, so it stays leader.
     */
    @Test
    void theLeaderOfMostPartitionsIsSlow() {
        start();
        int leader = cluster.leaderOfMostPartitions(topic);
        System.out.println("e: slowing node " + leader + ", leaders " + cluster.leaders(topic));
        cluster.slow(leader, Duration.ofMillis(500));
        hold(Duration.ofSeconds(25));
        cluster.slow(leader, Duration.ZERO);
        finish("e-slow-leader", Long.MIN_VALUE);
    }

    /**
     * (f) the topic deleted mid-flight, auto-create off, created again 20 s later. What a handler call that began
     * before the new topic's creation delivered may have gone with the old one, so those ids are not expected; every
     * other committed id is. Of them, those of calls that began once no broker listed the topic any more cannot have
     * been appended to the old one: each must be in the new one.
     */
    @Test
    void theTopicIsDeletedAndCreatedAgain() {
        start();
        cluster.deleteTopic(topic);
        long deleted = System.nanoTime();
        hold(Duration.ofSeconds(20));
        long recreating = System.nanoTime();
        createTopic();
        var seen = finish("f-topic-recreated", recreating);
        var whileDeleted = deliveredInCallFrom.entrySet().stream()
                .filter(e -> e.getValue() >= deleted && e.getValue() < recreating).map(Map.Entry::getKey).toList();
        var absent = whileDeleted.stream().filter(id -> !seen.contains(id)).toList();
        System.out.println("f-topic-recreated deletedWindow: delivered=" + whileDeleted.size()
                + " absent=" + absent.size());
        assertThat(absent).as("delivered by calls that began while the topic did not exist, missing from it").isEmpty();
    }

    /** (g) writes to the topic denied to the outbox's principal by an ACL for 20 s, then allowed again. */
    @Test
    void writesAreDeniedForAWhile() {
        start();
        cluster.denyWrites(topic, true);
        hold(Duration.ofSeconds(20));
        cluster.denyWrites(topic, false);
        finish("g-acl-denied", Long.MIN_VALUE);
    }

    /** A fresh topic and fleet, writers committing, and the warm-up. */
    private void start() {
        topic = "faults-" + UUID.randomUUID();
        createTopic();
        var config = config();
        runtimes = List.of(runtime(config), runtime(sameTables(config)));
        startWriting();
        sleep(WARMUP);
        assertThat(deliveredInCallFrom).as("delivered before the fault").isNotEmpty();
    }

    private void createTopic() {
        // the broker's append time, so that the latency measured is commit to topic
        cluster.createTopic(topic, PARTITIONS, Map.of("message.timestamp.type", "LogAppendTime"));
    }

    /** Keeps the fault for {@code duration} and notes the outbox's state at its end. */
    private void hold(Duration duration) {
        sleep(duration);
        duringFault = runtimes.get(0).admin().status();
    }

    /**
     * Stops the writers after {@link #AFTER_HEAL} and checks the topic; ids delivered by a handler call that began
     * before {@code mayBeGoneBefore} (nanoTime) are not expected. Returns the ids read.
     */
    private Set<UUID> finish(String name, long mayBeGoneBefore) {
        sleep(AFTER_HEAL);
        stopWriting();

        var all = committed.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        Predicate<UUID> mayBeGone = id -> deliveredInCallFrom.getOrDefault(id, Long.MAX_VALUE) < mayBeGoneBefore;
        long stopped = System.nanoTime();
        var read = consume(all, mayBeGone, DRAIN);
        long drainMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopped);
        var notExpected = all.stream().filter(mayBeGone).collect(Collectors.toSet());
        var expected = new HashSet<>(all);
        expected.removeAll(notExpected);
        var admin = runtimes.get(0).admin();
        var settled = settle(admin);
        long dead = admin.countDeadLetters(DeadLetterFilter.any());

        var first = new HashMap<UUID, Seen>();
        read.forEach(r -> first.putIfAbsent(r.id(), r));
        var lost = expected.stream().filter(id -> !first.containsKey(id)).toList();
        long maxLatency = expected.stream().filter(first::containsKey)
                .mapToLong(id -> first.get(id).appendedAtMs() - committedAtMs.get(id)).max().orElse(0);
        var outOfOrder = keysOutOfOrder(read, expected);
        long goneWithTopic = notExpected.stream().filter(id -> !first.containsKey(id)).count();

        System.out.println(name + " sent=" + all.size() + " lost=" + lost.size() + " duplicates="
                + (read.size() - first.size()) + " maxLatencyMs=" + maxLatency);
        System.out.println(name + " details: drainMs=" + drainMs + " keysOutOfOrder=" + outOfOrder
                + " notExpected=" + notExpected.size() + " ofWhichAbsent=" + goneWithTopic + " dead=" + dead
                + extraDetails.get() + " during=" + brief(duringFault) + " after=" + brief(settled) + " failures=" + new TreeMap<>(
                        failures.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))));

        assertThat(lost).as(name + ": committed messages missing from the topic %s after the writers stopped", DRAIN)
                .isEmpty();
        assertThat(outOfOrder).as(name + ": keys whose first occurrences break send order").isEmpty();
        assertThat(settled.retrySize()).as(name + ": retry rows").isZero();
        assertThat(settled.parkedSize()).as(name + ": parked rows").isZero();
        assertThat(settled.waitingSize()).as(name + ": waiting rows").isZero();
        assertThat(dead).as(name + ": dead letters").isZero();
        return first.keySet();
    }

    /** The status once the retry, parked and waiting rows are gone, or after 30 s: deliveries commit after the send. */
    private static OutboxStatus settle(io.github.dsudomoin.outbox.OutboxAdmin admin) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        var status = admin.status();
        while ((status.retrySize() > 0 || status.parkedSize() > 0 || status.waitingSize() > 0)
                && System.nanoTime() < deadline) {
            sleep(Duration.ofMillis(200));
            status = admin.status();
        }
        return status;
    }

    private static String brief(OutboxStatus s) {
        return s == null ? "-" : "{lag=" + s.lagSeconds() + " retry=" + s.retrySize() + " parked=" + s.parkedSize()
                + " waiting=" + s.waitingSize() + " dead=" + s.deadLetters() + "}";
    }

    /** Keys whose first occurrences of the expected ids differ from their send order. */
    private List<String> keysOutOfOrder(List<Seen> read, Set<UUID> expected) {
        var firstOrder = new HashMap<String, LinkedHashSet<UUID>>();
        read.stream().filter(r -> expected.contains(r.id()))
                .forEach(r -> firstOrder.computeIfAbsent(r.key(), k -> new LinkedHashSet<>()).add(r.id()));
        return committed.entrySet().stream().filter(e -> {
            var sent = e.getValue().stream().filter(expected::contains).toList();
            var seen = List.copyOf(firstOrder.getOrDefault(e.getKey(), new LinkedHashSet<>()));
            // a lost id is reported as lost; the order is checked on what arrived
            return !sent.stream().filter(seen::contains).toList().equals(seen);
        }).map(Map.Entry::getKey).sorted().toList();
    }

    private void startWriting() {
        writing.set(true);
        pool = Executors.newFixedThreadPool(WRITERS);
        for (int w = 0; w < WRITERS; w++) {
            int writer = w;
            writers.add(pool.submit(() -> {
                var sequences = new int[KEYS_PER_WRITER];
                int next = 0;
                for (int tx = 0; writing.get(); tx++) {
                    var messages = new ArrayList<OutboxMessage>(MESSAGES_PER_TX);
                    for (int i = 0; i < MESSAGES_PER_TX; i++, next++) {
                        int k = next % KEYS_PER_WRITER;
                        var key = "w" + writer + "-k" + k;
                        messages.add(OutboxMessage.of(topic, key, (key + ":" + sequences[k]++).getBytes(UTF_8)));
                    }
                    var runtime = runtimes.get(tx % 2);
                    var ids = transactions.inTx(c -> runtime.outbox().sendAll(messages));
                    long at = System.currentTimeMillis();
                    for (int i = 0; i < ids.size(); i++) {
                        committedAtMs.put(ids.get(i), at);
                        committed.computeIfAbsent(new String(messages.get(i).key(), UTF_8), k -> new ArrayList<>())
                                .add(ids.get(i));
                    }
                    Thread.sleep(WRITE_PAUSE.toMillis());
                }
                return null;
            }));
        }
    }

    private void stopWriting() {
        writing.set(false);
        for (var w : writers) {
            try {
                w.get(30, TimeUnit.SECONDS); // a writer's failure fails the case
            } catch (Exception e) {
                throw new IllegalStateException("a writer failed", e);
            }
        }
    }

    private record Seen(UUID id, String key, long appendedAtMs) {}

    /**
     * Reads the topic from the start until every id of {@code expected} but those {@code excused} is there or
     * {@code timeout} passed.
     */
    private List<Seen> consume(Set<UUID> expected, Predicate<UUID> excused, Duration timeout) {
        var properties = new HashMap<>(cluster.clientProperties(false));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var missing = new HashSet<>(expected);
        var read = new ArrayList<Seen>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            var partitions = IntStream.range(0, PARTITIONS).mapToObj(p -> new TopicPartition(topic, p)).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                missing.removeIf(excused); // a call may report its deliveries after the read began
                if (missing.isEmpty()) {
                    break;
                }
                for (var r : consumer.poll(Duration.ofMillis(500))) {
                    var id = KafkaOutboxHeaders.messageId(r);
                    read.add(new Seen(id, new String(r.key(), UTF_8), r.timestamp()));
                    missing.remove(id);
                }
            }
        }
        return read;
    }

    private OutboxRuntime runtime(TestOutboxConfig config) {
        var properties = new Properties();
        properties.putAll(cluster.clientProperties(true));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "outbox-" + closing.size());
        var sender = new KafkaOutboxSender(() -> properties);
        closing.add(sender);
        var runtime = OutboxRuntime.builder(config, transactions, recording(sender.handler())).build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    /** {@code kafka}, noting when each delivered id's call began and what the failed records answered. */
    private OutboxHandler recording(KafkaOutboxHandler kafka) {
        return new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                kafka.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                long began = System.nanoTime();
                var outcomes = kafka.handleBatch(records);
                for (int i = 0; i < outcomes.size(); i++) {
                    var outcome = outcomes.get(i);
                    if (outcome == Outcome.delivered()) {
                        deliveredInCallFrom.putIfAbsent(records.get(i).messageId(), began);
                    } else {
                        failures.computeIfAbsent(describe(outcome), k -> new AtomicInteger()).incrementAndGet();
                        if (notEnoughReplicas(outcome)) {
                            notEnoughReplicasOutcomes.incrementAndGet();
                        }
                    }
                }
                return outcomes;
            }
        };
    }

    private static boolean notEnoughReplicas(Outcome outcome) {
        for (Throwable e = outcome instanceof Outcome.Retry r ? r.error() : null; e != null; e = e.getCause()) {
            if (e instanceof NotEnoughReplicasException || e instanceof NotEnoughReplicasAfterAppendException) {
                return true;
            }
        }
        return false;
    }

    private String describe(Outcome outcome) {
        Throwable error = outcome instanceof Outcome.Retry r ? r.error()
                : outcome instanceof Outcome.Permanent p ? p.error() : null;
        var name = outcome.getClass().getSimpleName();
        if (error == null) {
            return name;
        }
        var message = String.valueOf(error.getMessage()).replace(topic, "T").replaceAll("\\d+", "#");
        return name + ":" + error.getClass().getSimpleName() + ":"
                + message.substring(0, Math.min(90, message.length()));
    }

    /** A fresh table; the sender bounds a call by delivery.timeout.ms (20 s) + 5 s, so the relay gives it longer. */
    private static TestOutboxConfig config() {
        return new TestOutboxConfig().batchTimeout(Duration.ofSeconds(30)).deadLettersTimeout(Duration.ofSeconds(30));
    }

    private static TestOutboxConfig sameTables(TestOutboxConfig config) {
        return config().table(config.table()).inboxTable(config.inbox().table());
    }
}
