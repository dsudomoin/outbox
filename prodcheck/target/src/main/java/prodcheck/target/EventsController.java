package prodcheck.target;

import static prodcheck.common.Controls.json;
import static prodcheck.common.Controls.param;

import io.github.dsudomoin.outbox.Inbox;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prodcheck.common.Event;
import prodcheck.common.Ledger;
import prodcheck.common.Payload;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.http.common.HttpMethod;
import ru.tinkoff.kora.http.common.annotation.Header;
import ru.tinkoff.kora.http.common.annotation.HttpRoute;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.annotation.HttpController;
import ru.tinkoff.kora.json.common.annotation.Json;

@Component
@HttpController
public final class EventsController {

    @Json
    public record BatchItem(UUID id, String sink, String topic, @Nullable String key, long seq, String tag,
            int contract, String instance) {}

    @Json
    public record BatchResult(UUID id, int status) {}

    private static final Logger log = LoggerFactory.getLogger(EventsController.class);
    private static final long ONE_HOUR_MS = 3_600_000;

    private final JdbcConnectionFactory db;
    private final Inbox inbox;
    private final Fault fault;

    public EventsController(JdbcConnectionFactory db, Inbox inbox, Fault fault) {
        this.db = db;
        this.inbox = inbox;
        this.fault = fault;
    }

    @HttpRoute(method = HttpMethod.POST, path = "/events")
    public HttpServerResponse event(
            @Header("Idempotency-Key") String id,
            @Header("X-Sink") String sink,
            @Header("X-Topic") String topic,
            @Nullable @Header("X-Key") String key,
            @Header("X-Seq") String seq,
            @Header("X-Tag") String tag,
            @Header("X-Contract") String contract,
            @Header("X-Instance") String instance,
            byte[] body) throws Exception {
        var event = new Event(sink, UUID.fromString(id), topic, key, Long.parseLong(seq), tag, instance,
                Integer.parseInt(contract));
        return HttpServerResponse.of(process(event));
    }

    @HttpRoute(method = HttpMethod.POST, path = "/events/batch")
    @Json
    public List<BatchResult> batch(@Json List<BatchItem> items) {
        var results = new ArrayList<BatchResult>(items.size());
        for (var item : items) {
            var event = new Event(item.sink(), item.id(), item.topic(), item.key(), item.seq(), item.tag(),
                    item.instance(), item.contract());
            int status;
            try {
                status = process(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                status = 500;
            } catch (Exception e) {
                log.warn("event {} failed", item.id(), e);
                status = 500;
            }
            results.add(new BatchResult(item.id(), status));
        }
        return results;
    }

    /** A whole call's items in one transaction, for emptying a long tail in time: no faults apply. */
    @HttpRoute(method = HttpMethod.POST, path = "/events/batch-tx")
    @Json
    public List<BatchResult> batchTx(@Json List<BatchItem> items) {
        db.inTx(c -> {
            for (var item : items) {
                var event = new Event(item.sink(), item.id(), item.topic(), item.key(), item.seq(), item.tag(),
                        item.instance(), item.contract());
                Ledger.arrival(c, event);
                effectOrDouble(c, event);
            }
        });
        var results = new ArrayList<BatchResult>(items.size());
        for (var item : items) {
            results.add(new BatchResult(item.id(), 200));
        }
        return results;
    }

    @HttpRoute(method = HttpMethod.PUT, path = "/fault")
    public HttpServerResponse putFault(HttpServerRequest r) {
        try {
            fault.update(param(r, "mode"), param(r, "delayMs"), param(r, "failKeys"), param(r, "failPercent"),
                    param(r, "dropPercent"), param(r, "poisonFixed"), param(r, "countDoubles"));
            return json(200, fault.asMap());
        } catch (IllegalArgumentException e) {
            return json(400, Map.of("message", String.valueOf(e.getMessage())));
        }
    }

    @HttpRoute(method = HttpMethod.GET, path = "/fault")
    public HttpServerResponse getFault() {
        return json(200, fault.asMap());
    }

    @HttpRoute(method = HttpMethod.GET, path = "/health")
    public HttpServerResponse health() {
        return json(200, Map.of("status", "ok"));
    }

    /** The inbox let a known message through, which an effect's own key would stop: a row instead of an error, so it can be counted. */
    private static void effectOrDouble(Connection c, Event e) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_effect (sink, message_id, topic, key, seq, tag, contract)
                values (?, ?, ?, ?, ?, ?, ?) on conflict (sink, message_id) do nothing""")) {
            ps.setString(1, e.sink());
            ps.setObject(2, e.id());
            ps.setString(3, e.topic());
            ps.setString(4, e.key());
            ps.setLong(5, e.seq());
            ps.setString(6, e.tag());
            ps.setInt(7, e.contract());
            if (ps.executeUpdate() == 1) {
                return;
            }
        }
        try (var ps = c.prepareStatement("insert into pc_double_effect (sink, message_id) values (?, ?)")) {
            ps.setString(1, e.sink());
            ps.setObject(2, e.id());
            ps.executeUpdate();
        }
    }

    private int process(Event event) throws InterruptedException {
        db.inTx(c -> {
            Ledger.arrival(c, event);
        });

        var mode = fault.mode;
        if (mode.equals("error")) {
            return 500;
        }
        if (mode.equals("hang")) {
            var delay = fault.delayMs > 0 ? fault.delayMs : ONE_HOUR_MS;
            var until = System.nanoTime() + delay * 1_000_000;
            while (fault.mode.equals("hang") && System.nanoTime() < until) {
                Thread.sleep(20);
            }
        } else if (fault.delayMs > 0) {
            Thread.sleep(fault.delayMs);
        }
        var random = ThreadLocalRandom.current();
        if (event.key() != null && fault.failKeys.contains(event.key())) {
            return 500;
        }
        if (fault.failPercent > 0 && random.nextDouble() * 100 < fault.failPercent) {
            return 500;
        }
        if (Payload.poisoned(event.tag()) && !fault.poisonFixed) {
            return 422;
        }

        db.inTx(c -> {
            if (inbox.scope(event.sink()).tryAccept(event.id())) {
                if (fault.countDoubles) {
                    effectOrDouble(c, event);
                } else {
                    Ledger.effect(c, event);
                }
            }
        });

        if (fault.dropPercent > 0 && random.nextDouble() * 100 < fault.dropPercent) {
            return 500; // the lost response: the effect is done, the caller retries
        }
        return 200;
    }
}
