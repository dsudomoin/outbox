package prodcheck.common;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRelay;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.http.common.body.HttpBody;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;

/** What the control routes do; each app's controller maps its routes here, because Kora generates controllers per app. */
public final class Controls {

    private static final Logger log = LoggerFactory.getLogger(Controls.class);

    private final JdbcConnectionFactory db;
    private final String app;
    private final String instance;
    private final Map<String, Sender> senders;
    private final Map<String, LoadGenerator> loads = new LinkedHashMap<>(); // one per outbox, each with its own writers
    private final HandlerControl handlers;

    public Controls(JdbcConnectionFactory db, String app, String instance, Map<String, Sender> senders,
            HandlerControl handlers) {
        this.db = db;
        this.app = app;
        this.instance = instance;
        this.senders = senders;
        senders.keySet().stream().sorted().forEach(outbox -> loads.put(outbox, new LoadGenerator(db, app, instance, senders)));
        this.handlers = handlers;
    }

    public HttpServerResponse startLoad(HttpServerRequest r) {
        try {
            var outbox = param(r, "outbox", "main");
            var topics = param(r, "topics");
            var settings = new LoadGenerator.Settings(
                    Integer.parseInt(param(r, "rate", "200")),
                    positive("writers", param(r, "writers", "4")),
                    positive("keys", param(r, "keys", "50")),
                    topics == null || topics.isBlank() ? null : List.of(topics.split(",")),
                    Double.parseDouble(param(r, "rollbackPercent", "0")),
                    parseTags(param(r, "tags", "")),
                    positive("txMessages", param(r, "txMessages", "1")),
                    Long.parseLong(param(r, "holdMs", "0")),
                    Boolean.parseBoolean(param(r, "bulk", "false")),
                    outbox,
                    param(r, "bulkKey"));
            var load = loads.get(outbox);
            if (load == null) {
                throw new IllegalArgumentException("unknown outbox " + outbox + ", this app has " + loads.keySet());
            }
            load.start(settings);
            return stats();
        } catch (IllegalArgumentException e) {
            return json(400, Map.of("message", String.valueOf(e.getMessage())));
        }
    }

    /** Stops the writers of {@code outbox=}, or all of them. */
    public HttpServerResponse stopLoad(HttpServerRequest r) {
        var outbox = param(r, "outbox");
        loads.forEach((name, load) -> {
            if (outbox == null || outbox.equals(name)) {
                load.stop();
            }
        });
        return stats();
    }

    /** The main outbox's writers, the others' under their names. */
    public HttpServerResponse stats() {
        var map = new LinkedHashMap<>(loads.get("main").stats());
        loads.forEach((name, load) -> {
            if (!name.equals("main")) {
                map.put(name, load.stats());
            }
        });
        return json(200, map);
    }

    public HttpServerResponse txSend(HttpServerRequest r) {
        var sender = senderOf(r);
        if (sender == null) {
            return json(400, Map.of("message", "unknown outbox"));
        }
        var topic = param(r, "topic");
        if (topic == null) {
            return json(400, Map.of("message", "topic is required"));
        }
        try {
            UUID id = db.inTx(c -> {
                return sendOne(c, sender, topic, param(r, "key"), param(r, "tag", "normal"), param(r, "raw"));
            });
            return json(200, Map.of("id", id.toString()));
        } catch (Exception e) {
            return refusal(e);
        }
    }

    public HttpServerResponse txHold(HttpServerRequest r) {
        var sender = senderOf(r);
        var topic = param(r, "topic");
        var ms = param(r, "ms");
        if (sender == null || topic == null || ms == null) {
            return json(400, Map.of("message", "ms, topic and a known outbox are required"));
        }
        var key = param(r, "key");
        var tag = param(r, "tag", "normal");
        var sent = new CompletableFuture<UUID>();
        var thread = new Thread(() -> {
            try {
                db.inTx(c -> {
                    sent.complete(sendOne(c, sender, topic, key, tag, null));
                    try {
                        Thread.sleep(Long.parseLong(ms));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("interrupted while holding the transaction", e);
                    }
                });
            } catch (Throwable t) {
                if (!sent.completeExceptionally(t)) {
                    log.warn("held transaction failed after its send", t);
                }
            }
        }, "tx-hold");
        thread.setDaemon(true);
        thread.start();
        try {
            return json(200, Map.of("id", sent.get(30, TimeUnit.SECONDS).toString()));
        } catch (Exception e) {
            return refusal(e);
        }
    }

    public HttpServerResponse txOutside(HttpServerRequest r) {
        var sender = senderOf(r);
        if (sender == null) {
            return json(400, Map.of("message", "unknown outbox"));
        }
        var result = new LinkedHashMap<String, Object>();
        try {
            result.put("id", sender.send(param(r, "topic", sender.topics().get(0)), "k0", 0, "normal").toString());
            result.put("exception", null);
        } catch (Exception e) {
            result.put("exception", LoadGenerator.rootCause(e).getClass().getName());
        }
        return json(200, result);
    }

    public HttpServerResponse putHandler(HttpServerRequest r) {
        try {
            handlers.update(param(r, "route"), param(r, "mode"), param(r, "delayMs"), param(r, "failKeys"),
                    param(r, "poisonFixed"), param(r, "waiting"), param(r, "oneRound"));
            return json(200, handlers.asMap());
        } catch (IllegalArgumentException e) {
            return json(400, Map.of("message", String.valueOf(e.getMessage())));
        }
    }

    /** {@code OutboxRelay.drain()} on this request's thread: what it returned, or what it threw, and how long it took. */
    public static HttpServerResponse drain(OutboxRelay relay) {
        var started = System.nanoTime();
        var body = new LinkedHashMap<String, Object>();
        int status = 200;
        try {
            body.put("delivered", relay.drain());
        } catch (Exception e) {
            status = 409;
            var root = LoadGenerator.rootCause(e);
            body.put("exception", e.getClass().getName());
            body.put("rootCause", root.getClass().getName());
            body.put("message", e.getMessage());
        }
        body.put("ms", (System.nanoTime() - started) / 1_000_000);
        return json(status, body);
    }

    public HttpServerResponse getHandler() {
        return json(200, handlers.asMap());
    }

    /** {@code raw}: past the app's typed path, {@code garbage} as bytes no decoder reads, {@code null} as a tombstone. */
    private UUID sendOne(Connection c, Sender sender, String topic, @Nullable String key, String tag, @Nullable String raw)
            throws SQLException {
        var seq = key == null ? 0 : Ledger.nextSeq(c, topic, key);
        var id = raw == null ? sender.send(topic, key, seq, tag) : sender.outbox().send(OutboxMessage.of(topic, key,
                raw.equals("null") ? null : "not json".getBytes(StandardCharsets.UTF_8)));
        // raw bytes carry no contract header: version 1 whatever the release writes
        Ledger.sent(c, List.of(new Ledger.Sent(id, app, instance, sender.table(), topic, key, seq, tag,
                raw == null ? sender.contract(topic) : 1)));
        return id;
    }

    @Nullable
    private Sender senderOf(HttpServerRequest r) {
        return senders.get(param(r, "outbox", "main"));
    }

    private static HttpServerResponse refusal(Exception e) {
        var root = LoadGenerator.rootCause(e);
        var body = new LinkedHashMap<String, Object>();
        body.put("exception", root.getClass().getName());
        body.put("message", root.getMessage());
        return json(409, body);
    }

    private static int positive(String name, String value) {
        var n = Integer.parseInt(value);
        if (n < 1) {
            throw new IllegalArgumentException(name + " must be at least 1, got " + value);
        }
        return n;
    }

    /** {@code poison:500,requeue:300}: every 500th message gets poison, every 300th requeue. */
    private static Map<String, Integer> parseTags(String tags) {
        var map = new LinkedHashMap<String, Integer>();
        for (var part : tags.split(",")) {
            if (!part.isBlank()) {
                var nameEvery = part.split(":");
                if (nameEvery.length != 2) {
                    throw new IllegalArgumentException("tags are name:every, got " + part);
                }
                map.put(nameEvery[0], positive("every", nameEvery[1]));
            }
        }
        return map;
    }

    @Nullable
    public static String param(HttpServerRequest r, String name) {
        var values = r.queryParams().get(name);
        if (values == null) {
            return null;
        }
        return values.isEmpty() ? "" : values.iterator().next(); // Kora drops the empty value of `failKeys=`, not the key
    }

    public static String param(HttpServerRequest r, String name, String orElse) {
        var value = param(r, name);
        return value == null ? orElse : value;
    }

    public static HttpServerResponse json(int status, Object body) {
        return HttpServerResponse.of(status, HttpBody.json(Json.write(body)));
    }
}
