package io.github.dsudomoin.outbox.admin.kora;

import static io.github.dsudomoin.outbox.DeadLetter.Status.KEPT;
import static io.github.dsudomoin.outbox.DeadLetter.Status.PENDING;
import static io.github.dsudomoin.outbox.DeadLetter.Status.SCHEDULED;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.internal.HeaderCodec;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.testing.AdminRows;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServer;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/**
 * Every admin operation over HTTP against the real {@code OutboxAdmin}, the relay disabled so nothing moves by
 * itself. Each test has its own outbox table and server.
 */
@KoraAppTest(value = AdminTestApp.class, components = UndertowHttpServer.class)
class AdminHttpIT implements KoraAppTestConfigModifier {

    private static final String API = "/kora-outbox/admin/v1";
    private static final Pattern LETTER_ID = Pattern.compile("\\{\"id\":(\\d+),\"messageId\"");
    private static final Instant T0 = Instant.parse("2026-01-15T10:00:00Z");

    @TestComponent
    private OutboxSchema schema;
    @TestComponent
    private JdbcDatabase db;

    private final HttpClient client = HttpClient.newHttpClient();
    private final int port = freePort();

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "admin-http"
                }
                outbox {
                  table = %s
                  shards = 2
                  relay { enabled = false, batchTimeout = 100ms } # a busy shard is waited for 5.1 s
                  partitions { xidsPerPartition = 1000000, premake = 2 }
                }
                httpServer { publicApiHttpPort = %d, privateApiHttpPort = %d }
                openapi.management.file = ["openapi/kora-outbox-admin.yaml"]
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), TestDb.uniqueTable(), port,
                freePort()));
    }

    @Test
    void statusMapsEveryField() throws Exception {
        rows().execute("insert into " + names().halt() + " (max_tick_xmax) values ('1')");
        rows().execute("insert into " + names().retry()
                + " (id, shard, message_id, topic, created_at, waiting_since) values"
                + " (1, 0, gen_random_uuid(), 't', now(), now() - interval '1 hour'),"
                + " (2, 0, gen_random_uuid(), 't', now(), null)");
        rows().letter(1, "a", "1", "boom", T0, KEPT);
        rows().letter(2, "a", "2", "boom", T0, KEPT);
        rows().letter(3, "a", "3", "boom", T0, PENDING);
        rows().letter(4, "b", "4", "boom", T0, SCHEDULED);
        var orphan = rows().foreignOrphan();
        // this app's own heartbeat, its first beat at start, the next one maintenanceInterval (30 s) later
        try (var c = TestDb.connect()) {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (queryLong(c, "select count(*) from " + names().instance()) == 0) {
                assertThat(System.nanoTime()).as("the heartbeat's row").isLessThan(deadline);
                Thread.sleep(20);
            }
        }
        rows().execute("delete from " + names().instance());
        rows().execute("insert into " + names().instance() + " (instance_id, shards, last_seen, hook_active, relay,"
                + " contracts, routes, expires_at) values ('host-b/7/0000beef', 2, '2026-01-15T10:00:00Z', true, true,"
                + " '{\"t2\": 2, \"t1\": 1}', '{t2,t1}', now() + interval '1 hour'), ('host-c/8/0000cafe', 2,"
                + " '2026-01-15T10:00:00Z', false, false, null, null, null), ('host-d/9/0000d00d', 2,"
                + " '2026-01-15T10:00:00Z', true, true, '{}', '{}', now() - interval '1 second')");

        var response = call("GET", "/status", null);

        assertJson(response, 200);
        var oldest = Pattern.compile("\"waitingOldestSeconds\":([-+0-9.E]+)").matcher(response.body());
        assertThat(oldest.find()).isTrue();
        assertThat(Double.parseDouble(oldest.group(1))).isBetween(3600.0, 3660.0);
        assertThat(response.body().replaceAll("\"maintenanceSinceSuccessSeconds\":[-+0-9.E]+",
                "\"maintenanceSinceSuccessSeconds\":S").replace(oldest.group(), "\"waitingOldestSeconds\":W"))
                .isEqualTo("{\"halted\":true,\"lagSeconds\":0.0,\"retrySize\":2,\"parkedSize\":0,"
                + "\"waitingSize\":1,\"waitingOldestSeconds\":W,"
                + "\"deadLetters\":[{\"topic\":\"a\",\"status\":\"kept\",\"count\":2},"
                + "{\"topic\":\"a\",\"status\":\"pending\",\"count\":1},"
                + "{\"topic\":\"b\",\"status\":\"scheduled\",\"count\":1}],"
                + "\"orphans\":[{\"table\":\"" + orphan + "\",\"kind\":\"foreign\"}],\"tailOverflow\":false,"
                + "\"premade\":2,\"maintenanceSinceSuccessSeconds\":S,\"shards\":2,\"instances\":[{\"id\":"
                + "\"host-b/7/0000beef\",\"shards\":2,\"lastSeen\":\"2026-01-15T10:00:00.000Z\","
                + "\"hookActive\":true,\"relay\":true,\"live\":true,\"contracts\":{\"t1\":1,\"t2\":2},\"routes\":[\"t1\",\"t2\"]},"
                + "{\"id\":\"host-c/8/0000cafe\",\"shards\":2,\"lastSeen\":\"2026-01-15T10:00:00.000Z\","
                + "\"hookActive\":false,\"relay\":false,\"live\":false},{\"id\":\"host-d/9/0000d00d\",\"shards\":2,"
                + "\"lastSeen\":\"2026-01-15T10:00:00.000Z\",\"hookActive\":true,\"relay\":true,\"live\":false,\"contracts\":{},"
                + "\"routes\":[]}]}");
    }

    @Test
    void findsByEachFilterFieldAndPages() throws Exception {
        long a = rows().letter(1, "t1", "k1", "disk full", T0.minusSeconds(7200), KEPT);
        long b = rows().letter(2, "t2", "ключ", "timeout", T0.minusSeconds(3600), PENDING);
        long c = rows().letter(3, "t1", "k3", "timeout", T0, SCHEDULED);

        assertThat(find("topic=t1")).containsExactly(a, c);
        assertThat(find("key=" + encode("ключ"))).as("the key's UTF-8 bytes").containsExactly(b);
        assertThat(find("error=time")).containsExactly(b, c);
        assertThat(find("failedFrom=" + encode(T0.minusSeconds(3600).toString()))).as("from inclusive")
                .containsExactly(b, c);
        assertThat(find("failedTo=" + encode(T0.minusSeconds(3600).atOffset(ZoneOffset.ofHours(3)).toString())))
                .as("to exclusive, any offset").containsExactly(a);
        assertThat(find("status=scheduled")).containsExactly(c);
        assertThat(find("topic=t1&error=timeout")).as("all fields must match").containsExactly(c);
        assertThat(find("")).containsExactly(a, b, c);

        var first = call("GET", "/dead-letters?limit=2", null);
        assertJson(first, 200);
        assertThat(ids(first.body())).containsExactly(a, b);
        assertThat(first.body()).endsWith("],\"nextAfterId\":" + b + "}");
        var last = call("GET", "/dead-letters?limit=2&afterId=" + b, null);
        assertThat(ids(last.body())).containsExactly(c);
        assertThat(last.body()).as("a short page is the last").doesNotContain("nextAfterId");
    }

    @Test
    void letterJsonCarriesEveryField() throws Exception {
        long id = rows().letter(7, "t", "k", "timeout", T0, PENDING);
        rows().execute("update " + names().dead() + " set redrives = 2, handler_attempts = 1 where id = " + id);

        var response = call("GET", "/dead-letters?topic=t", null);

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"items\":[{\"id\":7,\"messageId\":\"" + rows().messageId(id) + "\","
                + "\"topic\":\"t\",\"key\":\"" + base64("k") + "\",\"payload\":\"" + base64("p") + "\","
                + "\"headers\":[{\"name\":\"h1\",\"value\":\"" + base64("v") + "\"},{\"name\":\"h2\"}],"
                + "\"createdAt\":\"2026-01-15T09:00:00.000Z\",\"attempts\":3,"
                + "\"failedAt\":\"2026-01-15T10:00:00.000Z\",\"error\":\"timeout\",\"status\":\"pending\","
                + "\"redrives\":2,\"handlerAttempts\":1,\"dueAt\":\"2026-01-15T11:00:00.000Z\"}]}");
    }

    @Test
    void countsByTheFilter() throws Exception {
        rows().letter(1, "t1", "a", "boom", T0, KEPT);
        rows().letter(2, "t1", "b", "boom", T0, PENDING);
        rows().letter(3, "t2", "c", "boom", T0, KEPT);

        assertThat(call("GET", "/dead-letters/count", null).body()).isEqualTo("{\"count\":3}");
        var t1 = call("GET", "/dead-letters/count?topic=t1", null);
        assertJson(t1, 200);
        assertThat(t1.body()).isEqualTo("{\"count\":2}");
        assertThat(call("GET", "/dead-letters/count?topic=t1&status=kept", null).body()).isEqualTo("{\"count\":1}");
    }

    @Test
    void redrivesByIdsIntoTheOutbox() throws Exception {
        long a = rows().letter(1, "t", "a", "boom", T0, KEPT);
        long b = rows().letter(2, "t", "b", "boom", T0, PENDING);
        long other = rows().letter(3, "t", "c", "boom", T0, KEPT);
        var sent = List.of(rows().messageId(a), rows().messageId(b));

        var response = call("POST", "/dead-letters/redrive", "{\"ids\":[" + a + "," + b + ",999999]}");

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"count\":2}");
        assertThat(rows().deadIds()).containsExactly(other);
        assertThat(rows().outboxMessageIds()).containsExactlyElementsOf(sent);
    }

    @Test
    void redrivesByFilter() throws Exception {
        long a = rows().letter(1, "t1", "a", "boom", T0, KEPT);
        long other = rows().letter(2, "t2", "b", "boom", T0, KEPT);
        var sent = rows().messageId(a);

        var response = call("POST", "/dead-letters/redrive", "{\"filter\":{\"topic\":\"t1\"}}");

        assertThat(response.body()).isEqualTo("{\"count\":1}");
        assertThat(rows().deadIds()).containsExactly(other);
        assertThat(rows().outboxMessageIds()).containsExactly(sent);
    }

    @Test
    void discardsByFilterAndByIds() throws Exception {
        long a = rows().letter(1, "t1", "a", "disk full", T0, KEPT);
        rows().letter(2, "t1", "b", "timeout", T0, KEPT);
        rows().letter(3, "t2", "c", "timeout", T0, KEPT);
        long kept = rows().letter(4, "t2", "d", "disk full", T0, KEPT);

        var byFilter = call("POST", "/dead-letters/discard", "{\"filter\":{\"error\":\"timeout\"}}");
        var byIds = call("POST", "/dead-letters/discard", "{\"ids\":[" + a + "]}");

        assertJson(byFilter, 200);
        assertThat(byFilter.body()).isEqualTo("{\"count\":2}");
        assertThat(byIds.body()).isEqualTo("{\"count\":1}");
        assertThat(rows().deadIds()).containsExactly(kept);
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    @Test
    void selectionNeedsExactlyOneOfIdsAndFilter() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"ids\":[" + id + "],\"filter\":{\"topic\":\"t\"}}", "{}", "{\"filter\":{}}",
                    "{\"filter\":{\"topic\":\" \"}}", "{\"ids\":[null]}", "null")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"");
            }
        }
        assertThat(rows().deadIds()).containsExactly(id);
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    /** A blank topic or error dropped would widen the call to every letter the rest of the filter matches. */
    @Test
    void blankFilterStringsAreRefusedByBulkCalls() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);
        long other = rows().letter(2, "u", "k", "boom", T0, KEPT);

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"filter\":{\"topic\":\"\",\"status\":\"kept\"}}",
                    "{\"filter\":{\"topic\":\" \",\"status\":\"kept\"}}",
                    "{\"filter\":{\"error\":\"\",\"topic\":\"t\"}}")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"a blank ");
            }
        }
        assertThat(rows().deadIds()).containsExactly(id, other);
        assertThat(rows().outboxMessageIds()).isEmpty();
        assertThat(find("topic=&status=kept")).as("reads still take a blank one as absent").containsExactly(id, other);
    }

    /** A typo must never widen a call: {@code eror} ignored would discard every letter of the topic. */
    @Test
    void unknownFieldsAreRefused() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"filter\":{\"topic\":\"t\",\"eror\":\"x\"}}",
                    "{\"filter\":{\"topic\":\"t\"},\"limit\":1}", "{\"idz\":[1],\"filter\":{\"topic\":\"t\"}}")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"unknown field '");
            }
        }
        assertThat(rows().deadIds()).containsExactly(id);
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    /** A repeated field must never widen a call: the last {@code "error":null} kept would discard the whole topic. */
    @Test
    void duplicateFieldsAreRefused() throws Exception {
        long x = rows().letter(1, "t", "k", "x", T0, KEPT);
        long y = rows().letter(2, "t", "k", "y", T0, KEPT);
        rows().seedWaiting();

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"filter\":{\"topic\":\"t\",\"error\":\"x\",\"error\":null}}",
                    "{\"ids\":[" + x + "],\"ids\":null,\"filter\":{\"topic\":\"t\"}}")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"duplicate field '");
            }
        }
        for (var path : List.of("/waiting/abandon", "/keys/abandon")) {
            var response = call("POST", path, "{\"topic\":\"t\",\"key\":\"k\",\"key\":null,\"reason\":\"r\"}");

            assertJson(response, 400);
            assertThat(response.body()).as(path).startsWith("{\"message\":\"duplicate field 'key' in Abandon");
        }
        assertThat(rows().deadIds()).containsExactly(x, y);
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    @Test
    void idsAreCappedAt1000() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);
        var thousand = LongStream.rangeClosed(1, 1000).mapToObj(Long::toString).collect(Collectors.joining(","));

        var tooMany = call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + ",1001]}");

        assertJson(tooMany, 400);
        assertThat(tooMany.body()).startsWith("{\"message\":\"at most 1000 ids");
        assertThat(rows().deadIds()).containsExactly(id);
        assertThat(call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + "]}").body())
                .isEqualTo("{\"count\":1}");
    }

    @Test
    void restoresAStandaloneRangeFromTheCut() throws Exception {
        var orphan = rows().foreignOrphan();
        rows().orphanRow(orphan, 1, 10);
        var at = rows().orphanRow(orphan, 2, 20);
        var above = rows().orphanRow(orphan, 3, 30);

        var response = call("POST", "/orphans/" + orphan + "/restore?fromXid=20", null);

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"count\":2}");
        assertThat(rows().exists(orphan)).as("dropped").isFalse();
        assertThat(rows().outboxMessageIds()).containsExactly(at, above);
    }

    @Test
    void aNegativeFromXidIs400AndKeepsTheOrphan() throws Exception {
        var orphan = rows().foreignOrphan();
        rows().orphanRow(orphan, 1, 10);

        var response = call("POST", "/orphans/" + orphan + "/restore?fromXid=-1", null);

        assertJson(response, 400);
        assertThat(response.body()).startsWith("{\"message\":\"");
        assertThat(rows().exists(orphan)).isTrue();
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    @Test
    void tableNameIsNeverInterpolated() throws Exception {
        var victim = TestDb.uniqueTable();
        rows().execute("create table " + victim + " (id bigint)");
        var orphan = rows().foreignOrphan();
        rows().orphanRow(orphan, 1, 10);

        for (var table : List.of(orphan + ";drop table " + victim, names().base() + "_x1000' or '1'='1",
                victim, names().base(), "otherschema." + orphan)) {
            var response = call("POST", "/orphans/" + encode(table).replace("+", "%20") + "/restore?fromXid=0",
                    null);

            assertJson(response, 400);
            assertThat(response.body()).as("the decoded name reached the call")
                    .startsWith("{\"message\":\"" + table + " is not ");
        }
        assertThat(rows().exists(victim)).isTrue();
        assertThat(rows().exists(orphan)).isTrue();
        assertThat(rows().outboxMessageIds()).isEmpty();
    }

    @Test
    void recoversAnXidRegressionOnlyWhenThereIsOne() throws Exception {
        var none = call("POST", "/recovery/xid-regression", null);

        assertJson(none, 409);
        assertThat(none.body()).startsWith("{\"message\":\"no xid regression to recover");

        var outbox = new PgOutbox(new KoraOutboxTransactions(db), schema, 2, () -> {}, TestTelemetry.kora());
        var sent = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of("t", "a", bytes("1")), OutboxMessage.of("t", "b", bytes("2")),
                OutboxMessage.of("t", "c", bytes("3")))));
        rows().forgeRegression();

        var recovered = call("POST", "/recovery/xid-regression", null);

        assertJson(recovered, 200);
        assertThat(recovered.body()).isEqualTo("{\"count\":3}");
        assertThat(rows().outboxMessageIds()).as("re-sent with the same message ids").containsAll(sent);
        assertThat(call("GET", "/status", null).body()).startsWith("{\"halted\":false,");
    }

    @Test
    void repairsStrandedShardsOnlyWhenThereAreAny() throws Exception {
        var none = call("POST", "/recovery/stranded-shards", null);

        assertJson(none, 409);
        assertThat(none.body()).startsWith("{\"message\":\"no stranded shards");

        var outbox = new PgOutbox(new KoraOutboxTransactions(db), schema, 2, () -> {}, TestTelemetry.kora());
        var sent = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of("t", "a", bytes("1")), OutboxMessage.of("t", "b", bytes("2")),
                OutboxMessage.of("t", "c", bytes("3")))));
        rows().execute("insert into " + names().tick() + " (snapshot) values (pg_current_snapshot())");
        rows().execute("delete from " + names().tick() + " where id = (select min(tick_id) from "
                + names().cursor() + ")");
        rows().execute("delete from " + names().checkpoint());

        var repaired = call("POST", "/recovery/stranded-shards", null);

        assertJson(repaired, 200);
        assertThat(repaired.body()).isEqualTo("{\"count\":3}");
        assertThat(rows().outboxMessageIds()).as("re-sent with the same message ids").containsAll(sent);
        assertJson(call("POST", "/recovery/stranded-shards", null), 409);
    }

    @Test
    void abandonsWaitingKeysWhole() throws Exception {
        rows().seedWaiting();

        var dryRun = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"rolled back\",\"dryRun\":true}");
        assertJson(dryRun, 200);
        assertThat(dryRun.body()).isEqualTo("{\"keys\":3,\"records\":5}");
        assertThat(rows().deadIds()).as("a dry run moves nothing").isEmpty();

        var oneKey = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"key\":\"ключ\",\"reason\":\"rolled back\"}");
        assertJson(oneKey, 200);
        assertThat(oneKey.body()).as("the key's UTF-8 bytes").isEqualTo("{\"keys\":1,\"records\":1}");
        assertThat(rows().deadIds()).containsExactly(30L);

        var rest = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"rolled back\",\"dryRun\":false}");
        assertThat(rest.body()).isEqualTo("{\"keys\":2,\"records\":4}");
        assertThat(rows().deadIds()).containsExactly(5L, 10L, 11L, 20L, 30L);
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(distinct status || ' ' || error, ', ') from "
                    + names().dead())).isEqualTo("kept abandoned: rolled back");
            assertThat(queryLong(c, "select count(*) from " + names().retry())).isZero();
        }
    }

    @Test
    void abandonsAKeyOrAnUnkeyedRecord() throws Exception {
        rows().seedWaiting();

        var dryRun = call("POST", "/keys/abandon", "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\"stuck\",\"dryRun\":true}");
        assertJson(dryRun, 200);
        assertThat(dryRun.body()).isEqualTo("{\"keys\":1,\"records\":3}");
        assertThat(rows().deadIds()).isEmpty();

        var key = call("POST", "/keys/abandon", "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\"stuck\"}");
        assertJson(key, 200);
        assertThat(key.body()).as("head, follower and parked row").isEqualTo("{\"keys\":1,\"records\":3}");
        assertThat(rows().deadIds()).containsExactly(5L, 10L, 11L);

        String unkeyed;
        try (var c = TestDb.connect()) {
            unkeyed = queryString(c, "select message_id::text from " + names().retry() + " where id = 20");
        }
        var record = call("POST", "/keys/abandon",
                "{\"topic\":\"t\",\"messageId\":\"" + unkeyed + "\",\"reason\":\"poison\"}");
        assertThat(record.body()).isEqualTo("{\"keys\":1,\"records\":1}");
        assertThat(rows().deadIds()).containsExactly(5L, 10L, 11L, 20L);

        assertThat(call("POST", "/keys/abandon", "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\"stuck\"}").body())
                .as("a second call").isEqualTo("{\"keys\":0,\"records\":0}");
    }

    @Test
    void abandonKeyRefusesBadInput() throws Exception {
        rows().seedWaiting();
        for (var body : new String[] {
            "{\"topic\":\"t\",\"reason\":\"r\"}",
            "{\"topic\":\"t\",\"key\":\"k\",\"messageId\":\"" + UUID.randomUUID() + "\",\"reason\":\"r\"}",
            "{\"topic\":\"t\",\"messageId\":\"not-a-uuid\",\"reason\":\"r\"}",
            "{\"topic\":\"t\",\"kye\":\"k\",\"reason\":\"r\"}",
            "{\"topic\":\"t\",\"key\":\"k\"}",
            "{\"key\":\"k\",\"reason\":\"r\"}",
            "{\"topic\":\"\",\"key\":\"k\",\"reason\":\"r\"}",
            "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\" \"}"
        }) {
            assertJson(call("POST", "/keys/abandon", body), 400);
        }
        assertThat(rows().deadIds()).isEmpty();
    }

    @Test
    void abandonKeyOnABusyShardIsAConflict() throws Exception {
        rows().seedWaiting();
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            try (var ps = holder.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
                ps.setInt(1, names().internalNamespace());
                ps.setInt(2, TableNames.LOCK_SHARD_BASE);
                ps.execute();
            }

            var busy = call("POST", "/keys/abandon", "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\"stuck\"}");

            assertJson(busy, 409);
            assertThat(busy.body()).contains("busy, retry");
            holder.rollback();
        }
        assertThat(rows().deadIds()).isEmpty();
    }

    /** The one audit line names the caller and the reason, each value quoted and escaped, so none forges a part. */
    @Test
    void abandonAuditLineCarriesItsReason() throws Exception {
        rows().seedWaiting();

        HttpResponse<String> response;
        String log;
        try (var stderr = new CapturedStderr()) {
            response = call("POST", "/waiting/abandon", "{\"topic\":\"t\",\"key\":\"ключ\",\"reason\":"
                    + "\"v2 rolled back\\\", dryRun \\\"true\\r\\nINFO forged\"}");
            log = stderr.text();
        }

        assertJson(response, 200);
        var audit = log.lines().filter(l -> l.contains("INFO " + OutboxAdminInterceptor.class.getName() + " - "))
                .toList();
        assertThat(audit).singleElement().asString().endsWith("outbox admin POST \"" + API + "/waiting/abandon\""
                + " by \"tester\" with topic \"t\", key \"ключ\", reason \"v2 rolled back\\\", dryRun"
                + " \\\"true\\r\\nINFO forged\", dryRun \"false\": 200");
        assertThat(log).doesNotContain("\nINFO forged");
    }

    /** A call that destroys or moves letters or rows names what it selected: the filter, the ids (the first ten). */
    @Test
    void destructiveAuditLinesNameTheirSelection() throws Exception {
        var orphan = rows().foreignOrphan();
        rows().orphanRow(orphan, 1, 10);
        var ids = LongStream.rangeClosed(1, 12).boxed().toList();

        String log;
        try (var stderr = new CapturedStderr()) {
            assertJson(call("POST", "/dead-letters/discard", "{\"filter\":{\"topic\":\"orders\",\"key\":\"k\","
                    + "\"error\":\"boom\",\"failedFrom\":\"2026-01-15T13:00:00+03:00\","
                    + "\"failedTo\":\"2026-01-16T10:00:00Z\",\"status\":\"kept\"}}"), 200);
            assertJson(call("POST", "/dead-letters/redrive", "{\"filter\":{\"topic\":\"orders\"}}"), 200);
            assertJson(call("POST", "/dead-letters/redrive", "{\"ids\":" + ids.toString().replace(" ", "") + "}"),
                    200);
            assertJson(call("POST", "/dead-letters/discard", "{\"ids\":[7]}"), 200);
            assertJson(call("POST", "/orphans/" + orphan + "/restore?fromXid=10", null), 200);
            log = stderr.text();
        }

        var audit = log.lines().filter(l -> l.contains("INFO " + OutboxAdminInterceptor.class.getName() + " - "))
                .map(l -> l.substring(l.indexOf("outbox admin "))).toList();
        assertThat(audit).containsExactly(
                "outbox admin POST \"" + API + "/dead-letters/discard\" by \"tester\" with topic \"orders\", key"
                        + " \"k\", error \"boom\", failedFrom \"2026-01-15T10:00:00Z\", failedTo"
                        + " \"2026-01-16T10:00:00Z\", status \"kept\": 200",
                "outbox admin POST \"" + API + "/dead-letters/redrive\" by \"tester\" with topic \"orders\", key"
                        + " null, error null, failedFrom null, failedTo null, status null: 200",
                "outbox admin POST \"" + API + "/dead-letters/redrive\" by \"tester\" with count \"12\", ids"
                        + " \"[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...]\": 200",
                "outbox admin POST \"" + API + "/dead-letters/discard\" by \"tester\" with count \"1\", ids"
                        + " \"[7]\": 200",
                "outbox admin POST \"" + API + "/orphans/" + orphan + "/restore\" by \"tester\" with fromXid"
                        + " \"10\": 200");
        assertThat(log).as("the core line caps the ids too")
                .contains("redriveDeadLetters(12 ids [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...]) on ")
                .contains("discardDeadLetters(1 ids [7]) on ");
    }

    /** A typo must never widen the call: {@code kye} ignored would abandon every key of the topic. */
    @Test
    void abandonRefusesBadInput() throws Exception {
        rows().seedWaiting();

        for (var body : List.of("{\"reason\":\"r\"}", "{\"topic\":\"t\"}", "{\"topic\":\"t\",\"reason\":\" \"}",
                "{\"topic\":\"\",\"reason\":\"r\"}", "{\"topic\":\"t\",\"kye\":\"k\",\"reason\":\"r\"}",
                "{\"topic\":\"t\",\"reason\":\"r\",\"dryRun\":\"no\"}", "{\"topic\":1,\"reason\":\"r\"}", "null",
                "[]")) {
            var response = call("POST", "/waiting/abandon", body);

            assertJson(response, 400);
            assertThat(response.body()).as(body).startsWith("{\"message\":\"");
        }
        assertThat(call("POST", "/waiting/abandon", "{\"topic\":\"t\",\"kye\":\"k\",\"reason\":\"r\"}").body())
                .startsWith("{\"message\":\"unknown field 'kye'");
        assertThat(rows().deadIds()).isEmpty();
    }

    /** What the generated server or the call refuses for its input answers 400 before anything runs. */
    @Test
    void badInputIs400() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);

        for (var query : List.of("limit=0", "limit=1001")) {
            var response = call("GET", "/dead-letters?" + query, null);

            assertJson(response, 400);
            assertThat(response.body()).startsWith("{\"message\":\"limit must be within 1..1000");
        }
        var badLimit = call("GET", "/dead-letters?limit=x", null);
        assertJson(badLimit, 400);
        assertThat(badLimit.body()).isEqualTo("{\"message\":\"Query parameter 'limit' has invalid value: x\"}");
        var malformed = call("POST", "/dead-letters/discard", "not json");
        assertJson(malformed, 400);
        assertThat(malformed.body()).startsWith("{\"message\":\"Unrecognized token 'not'");

        var rejected = new ArrayList<HttpResponse<String>>();
        rejected.add(call("GET", "/dead-letters?afterId=x", null));
        for (var query : List.of("failedFrom=yesterday", "failedTo=2026-10-27", "status=PENDING", "status=gone")) {
            rejected.add(call("GET", "/dead-letters?" + query, null));
            rejected.add(call("GET", "/dead-letters/count?" + query, null));
        }
        rejected.add(call("POST", "/orphans/" + names().base() + "_x1000/restore", null)); // fromXid required
        rejected.add(call("POST", "/orphans/" + names().base() + "_x1000/restore?fromXid=x", null));
        rejected.add(call("POST", "/dead-letters/redrive", "{\"ids\":[\"x\"]}"));
        rejected.add(call("POST", "/dead-letters/redrive", null));
        for (var response : rejected) {
            assertJson(response, 400);
            assertThat(response.body()).as(response.uri().toString()).startsWith("{\"message\":\"");
        }
        assertThat(rows().deadIds()).containsExactly(id);
    }

    /**
     * A page elsewhere can post {@code text/plain} or a form without asking the server first, and Kora would read the
     * JSON in it: the body routes take {@code application/json} only, as Spring MVC's do.
     */
    @Test
    void anotherContentTypeIs415() throws Exception {
        long id = rows().letter(1, "t", "k", "boom", T0, KEPT);

        for (var type : List.of("text/plain", "application/x-www-form-urlencoded")) {
            var response = send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + API
                    + "/dead-letters/discard")).header("Content-Type", type)
                    .POST(BodyPublishers.ofString("{\"ids\":[" + id + "]}")));

            assertJson(response, 415);
            assertThat(response.body()).isEqualTo("{\"message\":\"Content-Type " + type
                    + " is not supported: application/json only\"}");
        }
        assertThat(rows().deadIds()).containsExactly(id);
    }

    /**
     * Over plain HTTP to an address other than localhost a browser sends no {@code Sec-Fetch-Site}; its
     * {@code Origin} still tells another site's page from the server's own, by the {@code Host} the server got.
     */
    @Test
    void aPostFromAnotherOriginIs403() throws Exception {
        var foreign = send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + API
                + "/recovery/xid-regression")).header("Origin", "http://evil.example")
                .POST(BodyPublishers.noBody()));
        var own = send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + API
                + "/recovery/xid-regression")).header("Origin", "http://localhost:" + port)
                .POST(BodyPublishers.noBody()));

        assertJson(foreign, 403);
        assertThat(foreign.body()).isEqualTo("{\"message\":\"cross-site request refused\"}");
        assertJson(own, 409); // reached the route: there is no regression to recover
    }

    /**
     * Neither bad input nor a refusal of the outbox's state: a 500 whose {@code Error} has the failure's message, for
     * the authenticated operator; one WARN with the exception; the audit line says 500.
     */
    @Test
    void otherFailuresAreA500Error() throws Exception {
        rows().execute("drop table " + names().dead() + " cascade");

        HttpResponse<String> response;
        String log;
        try (var stderr = new CapturedStderr()) {
            response = call("GET", "/dead-letters/count", null);
            log = stderr.text();
        }

        assertJson(response, 500);
        assertThat(response.body()).startsWith("{\"message\":\"").contains(names().dead() + "\\\" does not exist");
        assertThat(response.headers().firstValue("X-Test-Global")).as("the global interceptor saw a response")
                .hasValue("1");
        var call = "outbox admin GET \"" + API + "/dead-letters/count\" by \"tester\"";
        assertThat(log.lines().filter(l -> l.contains(" WARN ") && l.contains(call + " failed, answered 500")))
                .as("one WARN").hasSize(1);
        assertThat(log).contains("PSQLException").contains(call + ": 500");
    }

    // ---------------------------------------------------------------------------------------------------- fixtures

    private AdminRows rows() {
        return new AdminRows(TestDb::connect, names());
    }

    private TableNames names() {
        return schema.names();
    }

    // ------------------------------------------------------------------------------------------------------- HTTP

    private List<Long> find(String query) throws Exception {
        var response = call("GET", "/dead-letters?" + query, null);
        assertJson(response, 200);
        return ids(response.body());
    }

    private static List<Long> ids(String page) {
        return LETTER_ID.matcher(page).results().map(m -> Long.parseLong(m.group(1))).toList();
    }

    private HttpResponse<String> call(String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + API + path))
                .timeout(Duration.ofSeconds(60)) // another process can take this port on 127.0.0.1 and never answer
                .header("X-Api-Key", "secret")
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        return client.send(request.build(), BodyHandlers.ofString());
    }

    /** {@code request} with the test's key, sent as it is otherwise. */
    private HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return client.send(request.timeout(Duration.ofSeconds(60)).header("X-Api-Key", "secret").build(),
                BodyHandlers.ofString());
    }

    private static void assertJson(HttpResponse<String> response, int status) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                t -> assertThat(t).startsWith("application/json"));
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, UTF_8);
    }

    private static String base64(String s) {
        return Base64.getEncoder().encodeToString(bytes(s));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
