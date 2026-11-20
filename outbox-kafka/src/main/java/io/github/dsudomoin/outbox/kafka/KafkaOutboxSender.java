package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_METRICS_PUSH_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.github.dsudomoin.outbox.InternalApi;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.BatchClassifier;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.ShardProcessor;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers relayed records to Kafka. Each {@link #send} borrows one pooled idempotent producer. The first
 * send that fails after Kafka appended it closes the producer with a zero timeout from inside its callback: that aborts
 * every queued batch, so no later record of a failed (topic, key) is written after it, and the producer is
 * discarded. A send Kafka refuses before appending it (metadata wait in {@code partitionsFor} or {@code send}, a
 * client-side {@link RecordTooLargeException} or {@link BufferExhaustedException}) has nothing to abort: the producer
 * stays open, and the call skips the rest of the record's topic (topic faults) or (topic, key). After a metadata
 * timeout the call also skips topics its producer has not resolved before: one metadata wait per call, plus one per
 * topic the producer wrongly takes as resolved (see {@link Pooled}). Records of a topic or partition that recently
 * failed on its own are not attempted until its {@link Cooldown} ends: a partition cools after a retriable error
 * Kafka's I/O thread reports for one of its records, a topic after a metadata timeout,
 * {@link TopicAuthorizationException} or {@link UnknownTopicOrPartitionException}; a record delivered, or refused by
 * the broker ({@code Permanent} from Kafka's I/O thread), shows that its topic and partition answer, and ends their
 * cooldowns. Unkeyed records are left to Kafka's partitioner unless a partition of their topic cools; then the sender
 * picks one partition per call among those with a leader that do not cool.
 *
 * <p>The records of a call still unacknowledged when a producer batch of their partition expires
 * ({@link TimeoutException} from Kafka's I/O thread), and those whose partition Kafka chose, are remembered by message
 * id, as are those still in flight when an interrupt or the call's deadline ends it first, so this works whatever the
 * timeouts: at most 10 000, each until a call delivers it or has it refused, or for 10 minutes. A call that holds any
 * of them borrows its producer from a second pool, made with {@code batch.size=0} and otherwise the same properties:
 * Kafka then sizes each producer batch to its first record, so no other record joins it (with compression one may,
 * until a refusal splits the batch into single records). Thus a record the broker refuses becomes {@code Permanent}
 * even when it shared a producer batch before: Kafka splits a batch the broker refuses as too large, and kafka-clients
 * before 4.2 splits it by {@code batch.size}, which rebuilds a batch that fits {@code batch.size} as it was, until
 * {@code delivery.timeout.ms} expires all its records; in a batch of its own, the refused record fails with
 * {@link RecordTooLargeException}. Such a call sends a partition at most {@code max.in.flight.requests.per.connection}
 * records per round trip; its producers are closed once nothing is remembered. With nothing remembered, a call looks
 * nothing up.
 *
 * <p>Producer properties are {@code driverProperties} with byte-array serializers, {@code enable.idempotence=true} and
 * {@code acks=all} forced; {@code max.block.ms} defaults to 5000, {@code enable.metrics.push} to false,
 * {@code delivery.timeout.ms} to 20000 and then, if unset, {@code request.timeout.ms} to 15000 (Kafka requires
 * {@code delivery.timeout.ms >= linger.ms + request.timeout.ms}). {@code transactional.id}, {@code partitioner.class}
 * and {@code partitioner.ignore.keys=true} are rejected: ordering and the cooldown need Kafka's per-key partitioning.
 *
 * <p>A call hands records over during its first 5 s, while Kafka can still answer them within
 * {@code delivery.timeout.ms} before the call's deadline; inside a relay call, also only while they could be answered
 * before the cut ahead of the relay's cancel (below), though never less than the relay call's first 5 s. It waits for
 * acks until {@code delivery.timeout.ms} + 5 s from entry (25 s by default). A {@code send()} that waited for buffer
 * space appends its record late, and Kafka's delivery timeout of the record starts there: the wait then runs
 * {@code delivery.timeout.ms} + 5 s from the last append, at most {@code delivery.timeout.ms} + 5 s +
 * {@code max.block.ms} from entry (30 s by default). Records not handed over in time are {@code NotAttempted}; the
 * relay reads them again in its next step. At 80 % of {@code batchTimeout} (24 s of 30 s by default) the relay asks its
 * handler call to wind down, with an interrupt: the sender stops handing records over, but keeps waiting for those in
 * flight and keeps the producer open, so they end with Kafka's own outcome. Inside a relay call it waits until 1 s
 * before the relay cancels the call at {@code batchTimeout}, no longer. A call still waiting then is cut: the sender
 * closes the producer and returns acked records as {@code Delivered}, unacked ones as {@code Retry} (remembered, as
 * above) and the rest as {@code NotAttempted}. The call is cut the same way at once when the relay gives up on it: past
 * {@code batchTimeout}, or when the relay's step is interrupted and rolls back (the relay stopping after its
 * {@code shutdownTimeout}, an interrupted {@code drain()}), even if that interrupt reaches the handler thread together
 * with the wind-down's. So is it on any other interrupt: a second one after the wind-down, the relay's handler threads
 * shutting down, a caller outside the relay. A cut turns slow sends into retries, which go unbatched until a call
 * settles them, and a new producer sends them again (a duplicate when Kafka had written them). Keep
 * {@code batchTimeout} at least {@code delivery.timeout.ms} + 5 s + {@code max.block.ms} so that they end as Kafka's
 * outcomes instead; the Kora and Spring adapters log a WARN at startup when it is shorter.
 *
 * <p>A record goes to Kafka with its headers but any named {@code outbox-local}, of whatever value: that name marks
 * a record for a route of the relay, and reaches no Kafka topic, a dead-letter topic included.
 */
public final class KafkaOutboxSender implements AutoCloseable {

    private static final Duration WAIT_SLACK = Duration.ofSeconds(5);
    /**
     * How long before the relay cancels its handler call a call ends, so that its outcomes still reach the relay: room
     * for a close that joins the I/O thread, a GC pause, and the way back through the handler.
     */
    private static final long RELAY_MARGIN = Duration.ofSeconds(1).toNanos();
    private static final Duration RELEASE_TIMEOUT = Duration.ofSeconds(5);
    private static final int EXPIRED_REMEMBERED = 10_000;
    private static final Duration REMEMBERED_FOR = Duration.ofMinutes(10);
    private static final Logger log = LoggerFactory.getLogger(KafkaOutboxSender.class);
    /** Numbers the client ids, unique across the senders of the JVM as Kafka's own {@code producer-<n>}. */
    private static final AtomicLong PRODUCERS_MADE = new AtomicLong();

    private final Supplier<Producer<byte[], byte[]>> factory;
    private final Supplier<Producer<byte[], byte[]>> unbatchedFactory;
    private final Duration wait;
    private final Duration longestCall;
    private final Clock clock;
    private final LongSupplier nanos; // measures the cooldowns and the memories; clock only dates cooldown ends
    private final Deque<Pooled> pool = new ConcurrentLinkedDeque<>();
    private final Deque<Pooled> unbatchedPool = new ConcurrentLinkedDeque<>();
    private final Cooldown<TopicPartition> cooldown;
    private final Cooldown<String> topicCooldown;
    /** The remembered records (see the class javadoc), by message id: when each was remembered, on {@code nanos}. */
    private final Map<UUID, Long> expired = new ConcurrentHashMap<>();
    private volatile long nextPurge; // on nanos
    private volatile boolean released;

    public KafkaOutboxSender(KafkaOutboxConfig config) {
        this(config, KafkaProducer::new);
    }

    /**
     * {@code producers} makes each producer the sender pools, from the validated properties (the forced and defaulted
     * ones set): the application's own Kafka client factory, e.g. one that adds its metrics or interceptors. It must
     * build each producer from the properties it receives: the sender passes {@code batch.size=0} for its second pool
     * (see the class javadoc), and a factory that builds from its own configuration silently loses that. A set
     * {@code client.id} arrives with a per-producer suffix unless {@link KafkaOutboxConfig#clientIdSuffix} is false.
     *
     * <p>Each call must return a new producer that nobody else uses, and {@code close(Duration.ZERO)} on it must close
     * it physically and abort its queued batches. A factory that shares or caches producers, such as spring-kafka's
     * {@code DefaultKafkaProducerFactory} (its {@code CloseSafeProducer} ignores {@code close}), does not fit; with
     * spring-kafka use {@code props -> new KafkaProducer<>(props)}, with interceptors in {@code interceptor.classes}.
     * The sender closes and replaces a producer after failures, so whatever the factory attaches to one (metrics bound
     * to it, say) must be released by that producer's {@code close}.
     */
    public KafkaOutboxSender(KafkaOutboxConfig config,
            Function<Map<String, Object>, Producer<byte[], byte[]>> producers) {
        this(producerConfig(config.driverProperties()), config.clientIdSuffix(), producers);
    }

    private KafkaOutboxSender(ProducerConfig config, boolean suffix,
            Function<Map<String, Object>, Producer<byte[], byte[]>> producers) {
        this(() -> producers.apply(properties(config, suffix ? "-outbox-" : null)), () -> {
            var unbatched = properties(config, suffix ? "-outbox-unbatched-" : null);
            unbatched.put(BATCH_SIZE_CONFIG, "0");
            return producers.apply(unbatched);
        }, Duration.ofMillis(config.getInt(DELIVERY_TIMEOUT_MS_CONFIG)),
                Duration.ofMillis(config.getLong(MAX_BLOCK_MS_CONFIG)), Clock.systemUTC(), System::nanoTime);
    }

    /**
     * A fresh copy of the properties for one producer. A set {@code client.id} becomes a prefix: Kafka registers a
     * producer's JMX metrics under its client id, so pooled producers sharing one would overwrite each other's metrics,
     * and closing any of them would unregister the metrics of the rest. A null {@code infix} leaves it as set.
     */
    private static Map<String, Object> properties(ProducerConfig config, @Nullable String infix) {
        var properties = config.originals();
        // an unset one Kafka replaces with a unique producer-<n>
        if (infix != null && properties.containsKey(CLIENT_ID_CONFIG)) {
            properties.put(CLIENT_ID_CONFIG,
                    config.getString(CLIENT_ID_CONFIG) + infix + PRODUCERS_MADE.incrementAndGet());
        }
        return properties;
    }

    /**
     * For tests: {@code nanos} (the {@link System#nanoTime} scale) measures the cooldowns, the remembered records and
     * the resolved topics, and {@code clock} dates the cooldowns' ends; {@code factory} makes the producers of both
     * pools.
     */
    KafkaOutboxSender(Supplier<Producer<byte[], byte[]>> factory, Duration deliveryTimeout, Clock clock,
            LongSupplier nanos) {
        this(factory, factory, deliveryTimeout, clock, nanos);
    }

    /** For tests: {@code unbatched} makes the producers of calls that hold remembered records. */
    KafkaOutboxSender(Supplier<Producer<byte[], byte[]>> factory, Supplier<Producer<byte[], byte[]>> unbatched,
            Duration deliveryTimeout, Clock clock, LongSupplier nanos) {
        this(factory, unbatched, deliveryTimeout, Duration.ZERO, clock, nanos);
    }

    /** For tests: {@code maxBlock} is the producers' {@code max.block.ms}. */
    KafkaOutboxSender(Supplier<Producer<byte[], byte[]>> factory,
            Supplier<Producer<byte[], byte[]>> unbatched, Duration deliveryTimeout, Duration maxBlock, Clock clock,
            LongSupplier nanos) {
        this.factory = factory;
        this.unbatchedFactory = unbatched;
        this.clock = clock;
        this.nanos = nanos;
        this.nextPurge = nanos.getAsLong();
        this.wait = deliveryTimeout.plus(WAIT_SLACK);
        this.longestCall = wait.plus(maxBlock); // a send or metadata wait may still block at the deadline
        this.cooldown = new Cooldown<>(deliveryTimeout); // a probe resolves within delivery.timeout.ms
        this.topicCooldown = new Cooldown<>(deliveryTimeout);
    }

    /** Validates {@code user} and returns the producer configuration (fails at startup, not on the first send). */
    static ProducerConfig producerConfig(Properties user) {
        // before kafka-clients 4, which has no default for it, rejects it as a ConfigException
        require(user.get(BOOTSTRAP_SERVERS_CONFIG) != null, BOOTSTRAP_SERVERS_CONFIG + " is required");
        require(!user.containsKey(TRANSACTIONAL_ID_CONFIG), TRANSACTIONAL_ID_CONFIG + " is not supported");
        require(!user.containsKey(PARTITIONER_CLASS_CONFIG),
                PARTITIONER_CLASS_CONFIG + " is not supported: ordering and the cooldown need Kafka's key hashing");
        var acks = user.get(ACKS_CONFIG);
        require(acks == null || Set.of("all", "-1").contains(String.valueOf(acks).trim()), "acks must be all, got " + acks);
        var idempotence = user.get(ENABLE_IDEMPOTENCE_CONFIG);
        require(idempotence == null || !"false".equalsIgnoreCase(String.valueOf(idempotence).trim()),
                ENABLE_IDEMPOTENCE_CONFIG + " must be true");
        var maxInFlight = user.get(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION); // before Kafka rejects it less clearly
        require(maxInFlight == null || Integer.parseInt(String.valueOf(maxInFlight).trim()) <= 5,
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " must be at most 5, got " + maxInFlight);

        var properties = new Properties();
        properties.putAll(user);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ACKS_CONFIG, "all");
        properties.putIfAbsent(MAX_BLOCK_MS_CONFIG, "5000"); // metadata normally takes milliseconds
        // with push on, kafka-clients 3.9 logs an ERROR from the I/O thread after each close(Duration.ZERO)
        properties.putIfAbsent(ENABLE_METRICS_PUSH_CONFIG, "false");
        if (!properties.containsKey(DELIVERY_TIMEOUT_MS_CONFIG)) {
            properties.put(DELIVERY_TIMEOUT_MS_CONFIG, "20000");
            properties.putIfAbsent(REQUEST_TIMEOUT_MS_CONFIG, "15000"); // Kafka's 30 s default exceeds 20 s
        }

        var config = new ProducerConfig(properties); // Kafka's own validation, e.g. retries=0 with idempotence
        require(!config.getList(BOOTSTRAP_SERVERS_CONFIG).isEmpty(), BOOTSTRAP_SERVERS_CONFIG + " is required");
        require(!config.getBoolean(PARTITIONER_IGNORE_KEYS_CONFIG),
                PARTITIONER_IGNORE_KEYS_CONFIG + " must be false: ordering and the cooldown need Kafka's key hashing");
        int delivery = config.getInt(DELIVERY_TIMEOUT_MS_CONFIG);
        long lingerAndRequest = config.getLong(LINGER_MS_CONFIG) + config.getInt(REQUEST_TIMEOUT_MS_CONFIG);
        require(delivery >= lingerAndRequest, DELIVERY_TIMEOUT_MS_CONFIG + " (" + delivery + ") must be at least "
                + LINGER_MS_CONFIG + " + " + REQUEST_TIMEOUT_MS_CONFIG + " (" + lingerAndRequest + ")");
        return config;
    }

    /**
     * One outcome per record, in list order. Records never handed to Kafka (after a failure closed the producer, after
     * an interrupt or the relay's wind-down, or past the hand-over cutoff) are {@code NotAttempted}, and so are records
     * that failed only because the broker refused another record of the call for good: they spend no attempt. An
     * {@link InvalidTopicException} is the topic's: every record of that topic not otherwise settled shares its
     * {@code Permanent}. Honours interrupts: returns early and keeps the flag; the relay's wind-down stops only the
     * hand-over (see the class javadoc).
     */
    public List<Outcome> send(List<OutboxRecord> records) {
        return sendAndReport(records).outcomes();
    }

    /**
     * {@link #send}'s outcomes, and whether a failure closed its producer while the broker refused a record of it for
     * good ({@code Permanent} from the I/O thread), before or after the close: the records in flight with it then
     * failed for that alone. A batch-mate ahead of the refused record fails first, with a generic
     * {@link KafkaException} that closes the call (Sender.java:806-822).
     */
    record Sent(List<Outcome> outcomes, boolean closedByRefusal) {}

    Sent sendAndReport(List<OutboxRecord> records) {
        if (records.isEmpty()) {
            return new Sent(List.of(), false);
        }
        long entry = System.nanoTime();
        boolean unbatched = anyRemembered() && records.stream().anyMatch(r -> expired.containsKey(r.messageId()));
        var home = unbatched ? unbatchedPool : pool;
        var pooled = home.pollFirst();
        if (pooled == null) {
            pooled = new Pooled((unbatched ? unbatchedFactory : factory).get(), home);
        }
        var call = new Call(pooled.producer, records.size(), unbatched, entry);
        try {
            var partitions = sendAll(call, records, handOverUntil(entry), pooled);
            long until = awaitUntil(entry, call.lastAppend);
            var ended = call.await(until);
            if (ended != Call.Ended.ACKED) {
                call.cut(switch (ended) {
                    case INTERRUPTED -> "interrupted";
                    case CANCELLED -> "the relay gave up on the call";
                    case INTERRUPTED_AFTER_WIND_DOWN -> "interrupted after the relay's wind-down";
                    default -> "not acknowledged within " + Duration.ofMillis(NANOSECONDS.toMillis(until - entry));
                });
            }
            return new Sent(outcomes(call, records, partitions), call.closedByRefusal());
        } catch (RuntimeException | Error e) {
            call.close("aborted by " + e); // unknown state: never reuse the producer
            throw e;
        } finally {
            if (!call.closed) {
                giveBack(pooled);
            }
            if (unbatched && expired.isEmpty()) { // none is needed now, after a closed call too
                drain(unbatchedPool);
            }
        }
    }

    /**
     * Until when a call that entered at {@code entry} hands records over (on {@link System#nanoTime}): while Kafka can
     * answer them before the call stops waiting, its delivery timeout before the deadline ({@link #WAIT_SLACK} from
     * entry) and, inside a relay call, before the cut ahead of the relay's cancel. A sender called late in a relay call
     * (a Kafka route after others in one {@code byTopic} call) then hands nothing over that the cut would leave in
     * flight, to be sent again by a new producer. A relay call too short for any record to get the whole delivery
     * timeout (a {@code batchTimeout} below the sender's budget: the startup WARN) keeps the first 5 s of the call, or
     * it would deliver nothing.
     */
    private long handOverUntil(long entry) {
        long until = entry + WAIT_SLACK.toNanos();
        var deadline = ShardProcessor.callDeadline();
        var started = ShardProcessor.callStarted();
        if (deadline.isEmpty() || started.isEmpty()) {
            return until;
        }
        long beforeTheCut = deadline.getAsLong() - RELAY_MARGIN - (wait.toNanos() - WAIT_SLACK.toNanos());
        long firstSeconds = started.getAsLong() + WAIT_SLACK.toNanos();
        return earlier(until, beforeTheCut - firstSeconds >= 0 ? beforeTheCut : firstSeconds);
    }

    /**
     * Until when a call that entered at {@code entry} waits for acks (on {@link System#nanoTime}): Kafka answers a
     * record within {@code delivery.timeout.ms} of its append, the end of its {@code send()}, which may wait for buffer
     * space up to {@code max.block.ms}; so {@link #wait} from the last append, at most {@link #longestCall} from entry,
     * and inside a relay call {@link #RELAY_MARGIN} before the relay cancels it, which would retry every record.
     */
    private long awaitUntil(long entry, long lastAppend) {
        long until = earlier(lastAppend + wait.toNanos(), entry + longestCall.toNanos());
        var relay = ShardProcessor.callDeadline();
        return relay.isPresent() ? earlier(until, relay.getAsLong() - RELAY_MARGIN) : until;
    }

    /** The earlier of two {@link System#nanoTime} values: safe across its overflow. */
    private static long earlier(long a, long b) {
        return a - b <= 0 ? a : b;
    }

    /**
     * Whether a record is remembered, once those remembered for {@link #REMEMBERED_FOR} are dropped (a look at most
     * once a second); one look at an empty map while none is.
     */
    private boolean anyRemembered() {
        if (expired.isEmpty()) {
            return false;
        }
        long now = nanos.getAsLong();
        if (now - nextPurge >= 0) { // differences: safe across the overflow of nanoTime
            nextPurge = now + SECONDS.toNanos(1);
            long rememberedFor = REMEMBERED_FOR.toNanos();
            expired.values().removeIf(at -> now - at - rememberedFor >= 0);
            if (expired.isEmpty()) {
                drain(unbatchedPool);
            }
        }
        return !expired.isEmpty();
    }

    private void giveBack(Pooled pooled) {
        pooled.home.offerFirst(pooled);
        if (released && pooled.home.remove(pooled)) { // close() has drained the pool already
            closeQuietly(pooled.producer, RELEASE_TIMEOUT);
        }
    }

    private static void drain(Deque<Pooled> pool) {
        for (Pooled pooled; (pooled = pool.pollFirst()) != null; ) {
            closeQuietly(pooled.producer, RELEASE_TIMEOUT);
        }
    }

    /**
     * Logs a WARN when a call's own budget exceeds {@code limit}, the value of {@code setting} that bounds a caller's
     * calls; {@code effect} says what then becomes of slow ones. Such calls are not lost: the sender returns their real
     * outcomes before the caller gives up on them (on the caller's interrupt, or just before the relay cancels its
     * call), but slow sends then end as retries. Internal, unstable: for the adapters of this repository.
     */
    @InternalApi
    public void warnIfLongerThan(String setting, Duration limit, String effect) {
        if (longestCall.compareTo(limit) > 0) {
            log.warn("A Kafka outbox call can take up to {} (delivery.timeout.ms + 5 s + max.block.ms), longer than "
                    + "{} ({}). {}. Raise {} or lower the producer timeouts in outbox.kafka.driverProperties",
                    longestCall, setting, limit, effect, setting);
        }
    }

    /**
     * Steps 1–3; returns the partition of each record the sender chose or computed (null: Kafka chooses). Hands no
     * record over from {@code handOverUntil} (on {@link System#nanoTime}), nor once the thread is interrupted or the
     * relay asks for a wind-down.
     */
    private TopicPartition[] sendAll(Call call, List<OutboxRecord> records, long handOverUntil, Pooled pooled) {
        var partitions = call.partitions;
        var partitionInfos = new HashMap<String, List<PartitionInfo>>();
        var unkeyed = new HashMap<String, Optional<TopicPartition>>(); // per topic, sticky for the call
        // decided once per topic and partition: another worker's success must not unblock a key whose earlier record
        // we skipped
        var coolingUntil = new HashMap<Object, Optional<Instant>>();
        var refused = new HashSet<Object>(); // topics and (topic, key)s whose record Kafka refused before appending it
        // the rest stays unsent (NotAttempted); an interrupt closes the producer only if records are in flight
        for (int i = 0; i < records.size() && !call.closed && !Thread.currentThread().isInterrupted()
                && !ShardProcessor.windDownRequested() && System.nanoTime() - handOverUntil < 0; i++) {
            var record = records.get(i);
            var topic = record.topic();
            call.started();
            try {
                if (!refused.isEmpty()
                        && (refused.contains(topic) || refused.contains(BatchClassifier.orderingKey(record)))) {
                    call.complete(i, Outcome.notAttempted(), false);
                    continue;
                }
                if (call.metadataTimedOut && !partitionInfos.containsKey(topic)
                        && !pooled.resolved(topic, nanos.getAsLong())) {
                    // one metadata wait per call; before the cooldown lookup, which would lease the topic to us
                    var until = notBefore(topicCooldown.peek(topic, nanos.getAsLong()));
                    call.complete(i, until.isPresent() ? Outcome.notAttempted(until.get()) : Outcome.notAttempted(),
                            false);
                    continue;
                }
                var topicUntil = coolingUntil.computeIfAbsent(topic,
                        t -> notBefore(topicCooldown.coolingUntil(topic, nanos.getAsLong())));
                if (topicUntil.isPresent()) {
                    call.complete(i, Outcome.notAttempted(topicUntil.get()), false);
                    continue;
                }
                var infos = partitionInfos.get(topic);
                if (infos == null) {
                    try {
                        infos = call.producer.partitionsFor(topic);
                    } catch (InterruptException e) {
                        throw e;
                    } catch (RuntimeException e) { // a metadata failure: the record was never handed to send()
                        pooled.forget(topic);
                        refuse(call, i, record, e, true, coolingUntil, refused);
                        continue;
                    }
                    partitionInfos.put(topic, infos);
                    pooled.used(topic, nanos.getAsLong());
                    if (System.nanoTime() - handOverUntil >= 0) { // a slow lookup: past the hand-over cutoff
                        call.complete(i, Outcome.notAttempted(), false);
                        break;
                    }
                }
                var topicPartitions = infos;
                var partition = record.key() != null
                        ? new TopicPartition(topic, BuiltInPartitioner.partitionForKey(record.key(), infos.size()))
                        : unkeyed.computeIfAbsent(topic, t -> unkeyedPartition(topic, topicPartitions, coolingUntil))
                                .orElse(null);
                if (partition != null) {
                    partitions[i] = partition;
                    var until = coolingUntil.computeIfAbsent(partition,
                            p -> notBefore(cooldown.coolingUntil(partition, nanos.getAsLong())));
                    if (until.isPresent()) {
                        call.complete(i, Outcome.notAttempted(until.get()), false);
                        continue;
                    }
                }
                var explicit = record.key() == null && partition != null ? partition.partition() : null;
                call.producer.send(producerRecord(record, explicit), call.callback(i));
                if (call.refused != null) {
                    refuse(call, i, record, call.refused, false, coolingUntil, refused);
                    call.refused = null;
                } else {
                    call.lastAppend = System.nanoTime(); // where Kafka's delivery timeout of the record starts
                }
            } catch (InterruptException e) { // waiting for metadata or buffer space: the record joined no batch
                call.complete(i, Outcome.notAttempted(), false);
            } catch (RuntimeException e) {
                call.failed(i, e, null);
            }
        }
        return partitions;
    }

    /**
     * I1: empty while no partition of {@code topic} has a cooldown entry, so Kafka's partitioner picks one; otherwise
     * a random partition with a leader that does not cool, or, when all of them cool, the one cooling least long.
     */
    private Optional<TopicPartition> unkeyedPartition(String topic, List<PartitionInfo> infos,
            Map<Object, Optional<Instant>> coolingUntil) {
        if (infos.stream().noneMatch(p -> cooldown.contains(new TopicPartition(topic, p.partition())))) {
            return Optional.empty();
        }
        TopicPartition earliest = null;
        Instant earliestUntil = null;
        int start = ThreadLocalRandom.current().nextInt(infos.size());
        for (int k = 0; k < infos.size(); k++) {
            var info = infos.get((start + k) % infos.size());
            if (info.leader() == null) {
                continue;
            }
            var partition = new TopicPartition(topic, info.partition());
            var until = coolingUntil.computeIfAbsent(partition,
                    p -> notBefore(cooldown.coolingUntil(partition, nanos.getAsLong()))); // may lease it
            if (until.isEmpty()) {
                return Optional.of(partition);
            }
            if (earliest == null || until.get().isBefore(earliestUntil)) {
                earliest = partition;
                earliestUntil = until.get();
            }
        }
        return Optional.ofNullable(earliest);
    }

    /**
     * C1: Kafka refused record {@code i} before appending it, so the producer has nothing to abort and stays open.
     * The rest of the call skips the record's topic after a metadata failure (cooling the topic after a metadata
     * timeout or a {@link #topicFault}) or an {@link InvalidTopicException}, whose {@code Permanent} the skipped
     * records of the topic then share ({@link #outcomes}), otherwise its (topic, key); a client-side
     * {@link RecordTooLargeException} is {@code Permanent} and skips nothing. Before the append a
     * {@link TimeoutException} is a metadata wait, unless it is a {@link BufferExhaustedException}; after one, the call
     * looks up no topic its producer has not resolved before, so it waits {@code max.block.ms} for metadata once, plus
     * once per topic wrongly taken as resolved ({@link Pooled}).
     */
    private void refuse(Call call, int i, OutboxRecord record, Exception e, boolean metadata,
            Map<Object, Optional<Instant>> coolingUntil, Set<Object> refused) {
        call.complete(i, outcome(e), false);
        var topic = record.topic();
        boolean metadataTimeout = e instanceof TimeoutException && !(e instanceof BufferExhaustedException);
        call.metadataTimedOut |= metadataTimeout;
        if (metadataTimeout || topicFault(e)) {
            coolingUntil.put(topic, notBefore(topicCooldown.failed(topic, nanos.getAsLong())));
        } else if (metadata || e instanceof InvalidTopicException) {
            refused.add(topic);
        } else if (!(e instanceof RecordTooLargeException)) {
            refused.add(BatchClassifier.orderingKey(record));
        }
    }

    /**
     * M3: errors that say the partition is unhealthy (timeouts, not-enough-replicas, not-leader, …). Not a generic
     * {@link KafkaException}, which a batch-mate of an invalid record gets (Sender.java:806-822).
     */
    private static boolean partitionFault(Throwable e) {
        return e instanceof RetriableException && !(e instanceof BufferExhaustedException);
    }

    /** Errors of the whole topic, not of a partition: they cool the topic. */
    private static boolean topicFault(Throwable e) {
        return e instanceof TopicAuthorizationException || e instanceof UnknownTopicOrPartitionException;
    }

    /** Steps 5–6. */
    private List<Outcome> outcomes(Call call, List<OutboxRecord> records, TopicPartition[] partitions) {
        long now = nanos.getAsLong();
        var result = new ArrayList<Outcome>(partitions.length);
        var failed = new HashSet<TopicPartition>();
        var failedTopics = new HashSet<String>();
        var notSent = Outcome.notAttempted();
        Outcome unfinished = null;
        Map<String, Outcome> invalidTopics = null; // a refused topic name: the topic's records unsent die with it
        synchronized (call) {
            boolean refusal = call.closedByRefusal();
            for (int i = 0; i < partitions.length; i++) {
                var outcome = call.outcomes[i];
                if (outcome == null && i >= call.started) {
                    outcome = notSent;
                } else if (outcome == null) {
                    if (unfinished == null) {
                        unfinished = Outcome.retry(new KafkaException("not acknowledged: " + call.closeReason));
                    }
                    outcome = unfinished;
                }
                // records that failed only because the broker refused another one cost no attempt: the ones the close
                // aborted or left unacked, and a batch-mate ahead of the refused record, which Kafka fails first with a
                // generic KafkaException (Sender.java:806-822)
                if (refusal && outcome instanceof Outcome.Retry retry
                        && (!call.ownFailure[i] || retry.error().getClass() == KafkaException.class)) {
                    outcome = notSent;
                }
                result.add(outcome);
                if (outcome instanceof Outcome.Permanent p && p.error() instanceof InvalidTopicException) {
                    if (invalidTopics == null) {
                        invalidTopics = new HashMap<>();
                    }
                    invalidTopics.putIfAbsent(records.get(i).topic(), outcome);
                }
                var partition = partitions[i] != null ? partitions[i] : call.partitionChosenByKafka(i);
                if (call.stalled[i] && outcome != Outcome.delivered()) {
                    if (expired.size() < EXPIRED_REMEMBERED) {
                        expired.put(records.get(i).messageId(), now);
                    }
                } else if (call.unbatched && (outcome == Outcome.delivered() || outcome instanceof Outcome.Permanent)) {
                    expired.remove(records.get(i).messageId()); // settled
                }
                // a refusal from the broker shows, like a delivery, that the partition answers (after the close too)
                if (outcome == Outcome.delivered() || call.ownFailure[i] && outcome instanceof Outcome.Permanent) {
                    topicCooldown.succeeded(records.get(i).topic());
                    if (partition != null) {
                        cooldown.succeeded(partition);
                    }
                } else if (call.ownFailure[i] && outcome instanceof Outcome.Retry retry) {
                    if (partition != null && partitionFault(retry.error())) {
                        failed.add(partition);
                    }
                    if (topicFault(retry.error())) {
                        failedTopics.add(records.get(i).topic());
                    }
                }
            }
        }
        // the name is invalid for every record of the topic: all of them go dead now, not one per call while the rest
        // come back NotAttempted
        for (int i = 0; invalidTopics != null && i < result.size(); i++) {
            var dead = invalidTopics.get(records.get(i).topic());
            if (dead != null && result.get(i) instanceof Outcome.NotAttempted) {
                result.set(i, dead);
            }
        }
        failed.forEach(p -> cooldown.failed(p, nanos.getAsLong()));
        failedTopics.forEach(t -> topicCooldown.failed(t, nanos.getAsLong()));
        return result;
    }

    /** The end of a cooldown {@code wait} long from now, as a {@code notBefore}; empty when nothing cools. */
    private Optional<Instant> notBefore(@Nullable Duration wait) {
        return wait == null ? Optional.empty() : Optional.of(clock.instant().plus(wait));
    }

    private static ProducerRecord<byte[], byte[]> producerRecord(OutboxRecord record, @Nullable Integer partition) {
        var headers = new ArrayList<Header>(record.headers().size() + 1);
        for (var h : record.headers()) {
            if (!h.name().equals(PgOutbox.LOCAL_HEADER)) {
                headers.add(new RecordHeader(h.name(), h.value()));
            }
        }
        headers.add(new RecordHeader(KafkaOutboxHeaders.MESSAGE_ID, record.messageId().toString().getBytes(UTF_8)));
        return new ProducerRecord<>(record.topic(), partition, null, record.key(), record.payload(), headers);
    }

    private static Outcome outcome(Exception e) {
        return e instanceof RecordTooLargeException || e instanceof InvalidRecordException
                        || e instanceof InvalidTopicException
                ? Outcome.permanent(e)
                : Outcome.retry(e);
    }

    /** The relay's handler over this sender: {@link KafkaOutboxHandler}. */
    public KafkaOutboxHandler handler() {
        return new KafkaOutboxHandler(this);
    }

    @Override
    public void close() {
        released = true;
        drain(pool);
        drain(unbatchedPool);
    }

    private static void closeQuietly(Producer<byte[], byte[]> producer, Duration timeout) {
        try {
            producer.close(timeout);
        } catch (InterruptException e) { // closed anyway, only the I/O thread was not joined; the flag stays set
            log.debug("Interrupted while closing an outbox Kafka producer", e);
        } catch (RuntimeException e) {
            log.warn("Failed to close an outbox Kafka producer", e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("outbox.kafka.driverProperties: " + message);
        }
    }

    /**
     * A pooled producer and the topics it has fetched metadata for, by last lookup: {@code partitionsFor} does not wait
     * for those. Kafka drops a topic unused for {@code metadata.max.idle.ms} (5 min by default) from its metadata, so
     * an entry is trusted for 5 min after its last lookup, and at most 10 000 are kept, the least recently used dropped
     * first. A topic wrongly taken as resolved (deleted since, or dropped by a {@code metadata.max.idle.ms} below
     * 5 min) can cost a call that already timed out one more metadata wait, and is then forgotten; one wrongly taken as
     * unresolved costs no wait: Kafka answers its lookup from cache, and after a timeout the call skips it. Used by
     * one call at a time.
     */
    private static final class Pooled {
        private static final long IDLE = Duration.ofMinutes(5).toNanos();
        private static final int MAX_TOPICS = 10_000;

        final Producer<byte[], byte[]> producer;
        final Deque<Pooled> home; // the pool it goes back to
        private final Map<String, Long> lastUse = new LinkedHashMap<>() { // least recently used first, on nanos
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > MAX_TOPICS;
            }
        };

        Pooled(Producer<byte[], byte[]> producer, Deque<Pooled> home) {
            this.producer = producer;
            this.home = home;
        }

        boolean resolved(String topic, long now) {
            var used = lastUse.get(topic);
            if (used != null && now - used < IDLE) {
                return true;
            }
            lastUse.remove(topic);
            return false;
        }

        void used(String topic, long now) {
            lastUse.remove(topic); // re-inserted last
            lastUse.put(topic, now);
        }

        void forget(String topic) {
            lastUse.remove(topic);
        }
    }

    /** State of one {@link #send} call, shared with the producer's callbacks. */
    private static final class Call {
        final Producer<byte[], byte[]> producer;
        final Thread caller = Thread.currentThread();
        final boolean unbatched; // its producer is from the unbatched pool
        /** The partition of each record the sender chose or computed (null: Kafka's); written by the caller. */
        final TopicPartition[] partitions;
        // guarded by this, like ownFailure, metadata, stalled, pending, started, closeReason, closedBy
        final Outcome[] outcomes;
        final boolean[] ownFailure;
        final RecordMetadata[] metadata;
        final boolean[] stalled; // not acknowledged when a batch of its partition expired, or when the call was cut
        int pending;
        int started; // records 0 until started were handed to the loop body, in order
        volatile boolean closed; // written under this
        String closeReason = "";
        @Nullable
        Exception closedBy; // the failure whose failed() closed the call, if one did
        @Nullable
        Exception refused; // caller thread only: what send() reported through the callback before appending
        boolean metadataTimedOut; // caller thread only
        long lastAppend; // caller thread only: when send() last returned an appended record, else the call's entry

        Call(Producer<byte[], byte[]> producer, int size, boolean unbatched, long entry) {
            this.producer = producer;
            this.unbatched = unbatched;
            this.lastAppend = entry;
            this.partitions = new TopicPartition[size];
            this.outcomes = new Outcome[size];
            this.ownFailure = new boolean[size];
            this.metadata = new RecordMetadata[size];
            this.stalled = new boolean[size];
        }

        synchronized void started() {
            pending++;
            started++;
        }

        synchronized void complete(int i, Outcome outcome, boolean own) {
            record(i, outcome, own, null);
        }

        /** Holds this. The first completion of a record wins. */
        private void record(int i, Outcome outcome, boolean own, @Nullable RecordMetadata kafka) {
            if (outcomes[i] == null) {
                outcomes[i] = outcome;
                ownFailure[i] = own;
                metadata[i] = kafka;
                pending--;
                notifyAll();
            }
        }

        /**
         * Holds this. A batch of record {@code i}'s partition expired: marks the records still in flight there or in
         * a partition Kafka chose, its batch-mates among them (the call returns before Kafka fails them).
         */
        private void stall(int i, @Nullable RecordMetadata kafka) {
            var expiredIn = partitions[i] != null ? partitions[i]
                    : kafka != null && kafka.partition() >= 0 ? new TopicPartition(kafka.topic(), kafka.partition())
                    : null;
            for (int j = 0; j < started; j++) {
                var p = partitions[j];
                stalled[j] |= outcomes[j] == null && (p == null || expiredIn == null || p.equals(expiredIn));
            }
        }

        /**
         * Holds this. Where Kafka's partitioner put record {@code i}, from its callback: on failure too, where
         * KafkaProducer's AppendCallbacks fills in the partition the accumulator chose (KafkaProducer.java:1575-1576,
         * RecordAccumulator.java:322).
         */
        @Nullable
        TopicPartition partitionChosenByKafka(int i) {
            var m = metadata[i];
            return m != null && m.partition() >= 0 ? new TopicPartition(m.topic(), m.partition()) : null;
        }

        /**
         * KafkaProducer.doSend reports an {@code ApiException} raised before the append through the callback, on the
         * caller thread inside {@code send()} (KafkaProducer.java:1105-1111); only {@code maybeAddPartition} runs after
         * the append, and it throws none of these types. The caller handles them when {@code send()} returns.
         */
        Callback callback(int i) {
            return (kafka, e) -> {
                if (e == null) {
                    synchronized (this) {
                        record(i, Outcome.delivered(), false, kafka);
                    }
                } else if (Thread.currentThread() == caller && (e instanceof TimeoutException
                        || e instanceof RecordTooLargeException || e instanceof InvalidTopicException
                        || e instanceof TopicAuthorizationException)) {
                    refused = e;
                } else {
                    failed(i, e, kafka);
                }
            };
        }

        /**
         * Records the failure and marks the call closed in one step, so the caller never sees the outcome without the
         * close and never pools the producer. The record's own failure is one the I/O thread reports before our close,
         * or a {@code Permanent} one after it: an abort is never {@code Permanent}, so that is the broker's refusal of a
         * record whose batch-mate ahead of it closed the call. One thrown on the caller thread cools nothing.
         */
        void failed(int i, Exception e, @Nullable RecordMetadata kafka) {
            boolean closeProducer;
            synchronized (this) {
                if (e instanceof TimeoutException && Thread.currentThread() != caller && !closed) {
                    stall(i, kafka); // a batch expired
                }
                var outcome = outcome(e);
                record(i, outcome, Thread.currentThread() != caller
                        && (!closed || outcome instanceof Outcome.Permanent), kafka);
                closeProducer = markClosed("an earlier send failed: " + e);
                if (closeProducer) {
                    closedBy = e;
                }
            }
            if (closeProducer) {
                closeQuietly(producer, Duration.ZERO);
            }
        }

        /** See {@link Sent}; read once the call {@link #await}ed its started records. */
        synchronized boolean closedByRefusal() {
            for (int i = 0; closedBy != null && i < started; i++) {
                if (ownFailure[i] && outcomes[i] instanceof Outcome.Permanent) {
                    return true;
                }
            }
            return false;
        }

        /** Idempotent. Callers must not hold this monitor: closing from the caller joins the I/O thread. */
        void close(String reason) {
            close(reason, false);
        }

        /**
         * {@link #close} on an interrupt or past the deadline: unless a failure closed the call first, the records
         * still in flight are marked like an expired batch's, so a batch Kafka keeps rebuilding is not sent again
         * whole.
         */
        void cut(String reason) {
            close(reason, true);
        }

        private void close(String reason, boolean stallInFlight) {
            boolean closeProducer;
            synchronized (this) {
                closeProducer = markClosed(reason);
                for (int j = 0; closeProducer && stallInFlight && j < started; j++) {
                    stalled[j] |= outcomes[j] == null;
                }
            }
            if (closeProducer) {
                closeQuietly(producer, Duration.ZERO);
            }
        }

        /** Holds this. True for the first caller, who then closes the producer outside the monitor. */
        private boolean markClosed(String reason) {
            if (closed) {
                return false;
            }
            closed = true;
            closeReason = reason;
            notifyAll();
            return true;
        }

        /** How {@link #await} ended. */
        enum Ended { ACKED, INTERRUPTED, CANCELLED, INTERRUPTED_AFTER_WIND_DOWN, LATE }

        /**
         * {@code ACKED} once every started record completed or the call closed; otherwise at {@code until} (on
         * {@link System#nanoTime}) or on an interrupt, the flag kept. The relay's wind-down (an interrupt while
         * {@link ShardProcessor#windDownRequested()}) is not one: the records in flight get until {@code until} for
         * Kafka's own outcome, as closing the producer would turn them into retries, which a new producer sends again
         * (a duplicate when Kafka had written them). Its interrupt is consumed and set again on return; the next one
         * ends the wait. So does the relay's cancel ({@link ShardProcessor#callCancelled()}), at once, though its
         * interrupt may come as one with the wind-down's: the relay has let the shard go, and a record still in flight
         * could be written after a later one of its key. A call closed by a failure waits for the rest of its records:
         * the close
         * aborts them at once, and the records of the failed batch behind the one that closed it bring their own
         * answers (a refusal among them).
         */
        synchronized Ended await(long until) {
            boolean windingDown = false;
            try {
                while (pending > 0 && (!closed || closedBy != null)) {
                    if (ShardProcessor.callCancelled()) {
                        return Ended.CANCELLED;
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        if (windingDown) {
                            return Ended.INTERRUPTED_AFTER_WIND_DOWN;
                        }
                        if (!ShardProcessor.windDownRequested()) {
                            return Ended.INTERRUPTED;
                        }
                        windingDown = true;
                        Thread.interrupted();
                    }
                    long left = until - System.nanoTime();
                    if (left <= 0) {
                        return Ended.LATE;
                    }
                    try {
                        NANOSECONDS.timedWait(this, left);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // seen at the top of the loop
                    }
                }
                return Ended.ACKED;
            } finally {
                if (windingDown) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
