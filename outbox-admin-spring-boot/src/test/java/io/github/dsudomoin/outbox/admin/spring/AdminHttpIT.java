package io.github.dsudomoin.outbox.admin.spring;

import static io.github.dsudomoin.outbox.DeadLetter.Status.KEPT;
import static io.github.dsudomoin.outbox.DeadLetter.Status.PENDING;
import static io.github.dsudomoin.outbox.DeadLetter.Status.SCHEDULED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.jayway.jsonpath.JsonPath;
import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.internal.HeaderCodec;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

/**
 * Every admin operation over MockMvc against the real {@code OutboxAdmin}, the relay disabled so nothing moves by
 * itself, on Boot's default (lenient) mappers. Each test has its own outbox table and context.
 */
class AdminHttpIT {

    private static final String API = "/kora-outbox/admin/v1";
    private static final Instant T0 = Instant.parse("2026-01-15T10:00:00Z");

    private WebApplicationContext context;
    private MockMvc mvc;
    private TableNames names;

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Boot {

        @Bean
        RecordingHandler handler() {
            return new RecordingHandler();
        }
    }

    /** Boot's datasource keys, no migrations, a fresh outbox table, no relay, a busy shard waited for 5.1 s. */
    private static String[] properties(String... more) {
        return Stream.concat(Stream.of("spring.datasource.url=" + TestDataSources.jdbcUrl(),
            "spring.datasource.username=" + TestDataSources.user(),
            "spring.datasource.password=" + TestDataSources.password(),
            "spring.flyway.enabled=false", "spring.liquibase.enabled=false",
            "outbox.table=" + TestSql.uniqueTable(), "outbox.shards=2", "outbox.relay.enabled=false",
            "outbox.relay.batch-timeout=100ms", "outbox.partitions.xids-per-partition=1000000",
            "outbox.partitions.premake=2"), Stream.of(more)).toArray(String[]::new);
    }

    private interface Body {
        void run() throws Exception;
    }

    private void app(Body body, String... properties) {
        app(new Class<?>[0], body, properties);
    }

    private void app(Class<?>[] configurations, Body body, String... properties) {
        new WebApplicationContextRunner()
            .withUserConfiguration(Boot.class).withUserConfiguration(configurations)
            .withPropertyValues(properties(properties)).run(started -> {
                assertThat(started).hasNotFailed();
                context = started;
                mvc = MockMvcBuilders.webAppContextSetup(started).build();
                names = TableNames.of(started.getBean(OutboxConfig.class).table());
                body.run();
            });
    }

    @Test
    void statusMapsEveryField() {
        app(() -> {
            execute("insert into " + names.halt() + " (max_tick_xmax) values ('1')");
            execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at, waiting_since) values"
                + " (1, 0, gen_random_uuid(), 't', now(), now() - interval '1 hour'),"
                + " (2, 0, gen_random_uuid(), 't', now(), null)");
            letter(1, "a", "1", "boom", T0, KEPT);
            letter(2, "a", "2", "boom", T0, KEPT);
            letter(3, "a", "3", "boom", T0, PENDING);
            letter(4, "b", "4", "boom", T0, SCHEDULED);
            var orphan = foreignOrphan();
            // this app's own heartbeat, its first beat at start, the next one maintenanceInterval (30 s) later
            try (var c = connect()) {
                long deadline = System.nanoTime() + 10_000_000_000L;
                while (TestSql.queryLong(c, "select count(*) from " + names.instance()) == 0) {
                    assertThat(System.nanoTime()).as("the heartbeat's row").isLessThan(deadline);
                    Thread.sleep(20);
                }
            }
            execute("delete from " + names.instance());
            execute("insert into " + names.instance() + " (instance_id, shards, last_seen, hook_active, relay,"
                + " contracts, routes) values ('host-b/7/0000beef', 2, '2026-01-15T10:00:00Z', true, true,"
                + " '{\"t2\": 2, \"t1\": 1}', '{t2,t1}'), ('host-c/8/0000cafe', 2, '2026-01-15T10:00:00Z', false,"
                + " false, null, null), ('host-d/9/0000d00d', 2, '2026-01-15T10:00:00Z', true, true, '{}', '{}')");

            var response = call("GET", "/status", null);

            assertJson(response, 200);
            var oldest = Pattern.compile("\"waitingOldestSeconds\":([-+0-9.E]+)").matcher(response.body);
            assertThat(oldest.find()).isTrue();
            assertThat(Double.parseDouble(oldest.group(1))).isBetween(3600.0, 3660.0);
            assertSameJson(response.body.replaceAll("\"maintenanceSinceSuccessSeconds\":[-+0-9.E]+",
                "\"maintenanceSinceSuccessSeconds\":\"S\"").replace(oldest.group(), "\"waitingOldestSeconds\":\"W\""),
                "{\"halted\":true,\"lagSeconds\":0.0,\"retrySize\":2,\"parkedSize\":0,"
                    + "\"waitingSize\":1,\"waitingOldestSeconds\":\"W\","
                    + "\"deadLetters\":[{\"topic\":\"a\",\"status\":\"kept\",\"count\":2},"
                    + "{\"topic\":\"a\",\"status\":\"pending\",\"count\":1},"
                    + "{\"topic\":\"b\",\"status\":\"scheduled\",\"count\":1}],"
                    + "\"orphans\":[{\"table\":\"" + orphan + "\",\"kind\":\"foreign\"}],\"tailOverflow\":false,"
                    + "\"premade\":2,\"maintenanceSinceSuccessSeconds\":\"S\",\"shards\":2,\"instances\":[{\"id\":"
                    + "\"host-b/7/0000beef\",\"shards\":2,\"lastSeen\":\"2026-01-15T10:00:00.000Z\","
                    + "\"hookActive\":true,\"relay\":true,\"contracts\":{\"t1\":1,\"t2\":2},"
                    + "\"routes\":[\"t1\",\"t2\"]},"
                    + "{\"id\":\"host-c/8/0000cafe\",\"shards\":2,\"lastSeen\":\"2026-01-15T10:00:00.000Z\","
                    + "\"hookActive\":false,\"relay\":false},{\"id\":\"host-d/9/0000d00d\",\"shards\":2,"
                    + "\"lastSeen\":\"2026-01-15T10:00:00.000Z\",\"hookActive\":true,\"relay\":true,\"contracts\":{},"
                    + "\"routes\":[]}]}");
        });
    }

    @Test
    void findsByEachFilterFieldAndPages() {
        app(() -> {
            long a = letter(1, "t1", "k1", "disk full", T0.minusSeconds(7200), KEPT);
            long b = letter(2, "t2", "ключ", "timeout", T0.minusSeconds(3600), PENDING);
            long c = letter(3, "t1", "k3", "timeout", T0, SCHEDULED);

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
            assertThat(ids(first.body)).containsExactly(a, b);
            assertThat(((Number) JsonPath.read(first.body, "$.nextAfterId")).longValue()).isEqualTo(b);
            var last = call("GET", "/dead-letters?limit=2&afterId=" + b, null);
            assertThat(ids(last.body)).containsExactly(c);
            assertThat(last.body).as("a short page is the last").doesNotContain("nextAfterId");
        });
    }

    @Test
    void letterJsonCarriesEveryField() {
        app(() -> {
            long id = letter(7, "t", "k", "timeout", T0, PENDING);
            execute("update " + names.dead() + " set redrives = 2, handler_attempts = 1 where id = " + id);

            var response = call("GET", "/dead-letters?topic=t", null);

            assertJson(response, 200);
            assertSameJson(response.body, letterJson(id, messageId(id)));
        });
    }

    private static String letterJson(long id, UUID messageId) {
        return "{\"items\":[{\"id\":" + id + ",\"messageId\":\"" + messageId + "\","
            + "\"topic\":\"t\",\"key\":\"" + base64("k") + "\",\"payload\":\"" + base64("p") + "\","
            + "\"headers\":[{\"name\":\"h1\",\"value\":\"" + base64("v") + "\"},{\"name\":\"h2\"}],"
            + "\"createdAt\":\"2026-01-15T09:00:00.000Z\",\"attempts\":3,"
            + "\"failedAt\":\"2026-01-15T10:00:00.000Z\",\"error\":\"timeout\",\"status\":\"pending\","
            + "\"redrives\":2,\"handlerAttempts\":1,\"dueAt\":\"2026-01-15T11:00:00.000Z\"}]}";
    }

    /** Milliseconds, always three digits, as the Kora adapter writes them, whatever Postgres keeps. */
    @Test
    void datesAreWrittenToTheMillisecondInUtc() {
        app(() -> {
            letter(1, "t", "k", "boom", T0.plusNanos(123_456_000), KEPT);
            letter(2, "t", "k", "boom", T0.plusNanos(120_000_000), KEPT);

            var body = call("GET", "/dead-letters", null).body;

            assertThat(JsonPath.<List<String>>read(body, "$.items[*].failedAt"))
                .containsExactly("2026-01-15T10:00:00.123Z", "2026-01-15T10:00:00.120Z");
        });
    }

    @Test
    void countsByTheFilter() {
        app(() -> {
            letter(1, "t1", "a", "boom", T0, KEPT);
            letter(2, "t1", "b", "boom", T0, PENDING);
            letter(3, "t2", "c", "boom", T0, KEPT);

            assertThat(call("GET", "/dead-letters/count", null).body).isEqualTo("{\"count\":3}");
            var t1 = call("GET", "/dead-letters/count?topic=t1", null);
            assertJson(t1, 200);
            assertThat(t1.body).isEqualTo("{\"count\":2}");
            assertThat(call("GET", "/dead-letters/count?topic=t1&status=kept", null).body)
                .isEqualTo("{\"count\":1}");
        });
    }

    @Test
    void redrivesByIdsIntoTheOutbox() {
        app(() -> {
            long a = letter(1, "t", "a", "boom", T0, KEPT);
            long b = letter(2, "t", "b", "boom", T0, PENDING);
            long other = letter(3, "t", "c", "boom", T0, KEPT);
            var sent = List.of(messageId(a), messageId(b));

            var response = call("POST", "/dead-letters/redrive", "{\"ids\":[" + a + "," + b + ",999999]}");

            assertJson(response, 200);
            assertThat(response.body).isEqualTo("{\"count\":2}");
            assertThat(deadIds()).containsExactly(other);
            assertThat(outboxMessageIds()).containsExactlyElementsOf(sent);
        });
    }

    @Test
    void redrivesByFilter() {
        app(() -> {
            long a = letter(1, "t1", "a", "boom", T0, KEPT);
            long other = letter(2, "t2", "b", "boom", T0, KEPT);
            var sent = messageId(a);

            var response = call("POST", "/dead-letters/redrive", "{\"filter\":{\"topic\":\"t1\"}}");

            assertThat(response.body).isEqualTo("{\"count\":1}");
            assertThat(deadIds()).containsExactly(other);
            assertThat(outboxMessageIds()).containsExactly(sent);
        });
    }

    @Test
    void discardsByFilterAndByIds() {
        app(() -> {
            long a = letter(1, "t1", "a", "disk full", T0, KEPT);
            letter(2, "t1", "b", "timeout", T0, KEPT);
            letter(3, "t2", "c", "timeout", T0, KEPT);
            long kept = letter(4, "t2", "d", "disk full", T0, KEPT);

            var byFilter = call("POST", "/dead-letters/discard", "{\"filter\":{\"error\":\"timeout\"}}");
            var byIds = call("POST", "/dead-letters/discard", "{\"ids\":[" + a + "]}");

            assertJson(byFilter, 200);
            assertThat(byFilter.body).isEqualTo("{\"count\":2}");
            assertThat(byIds.body).isEqualTo("{\"count\":1}");
            assertThat(deadIds()).containsExactly(kept);
            assertThat(outboxMessageIds()).isEmpty();
        });
    }

    @Test
    void selectionNeedsExactlyOneOfIdsAndFilter() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);

            for (var operation : List.of("redrive", "discard")) {
                for (var body : List.of("{\"ids\":[" + id + "],\"filter\":{\"topic\":\"t\"}}", "{}", "{\"filter\":{}}",
                    "{\"filter\":{\"topic\":\" \"}}", "{\"ids\":[null]}", "null")) {
                    var response = call("POST", "/dead-letters/" + operation, body);

                    assertJson(response, 400);
                    assertThat(response.body).as(operation + " " + body).startsWith("{\"message\":\"");
                }
            }
            assertThat(deadIds()).containsExactly(id);
            assertThat(outboxMessageIds()).isEmpty();
        });
    }

    /** A blank topic or error dropped would widen the call to every letter the rest of the filter matches. */
    @Test
    void blankFilterStringsAreRefusedByBulkCalls() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);
            long other = letter(2, "u", "k", "boom", T0, KEPT);

            for (var operation : List.of("redrive", "discard")) {
                for (var body : List.of("{\"filter\":{\"topic\":\"\",\"status\":\"kept\"}}",
                    "{\"filter\":{\"topic\":\" \",\"status\":\"kept\"}}",
                    "{\"filter\":{\"error\":\"\",\"topic\":\"t\"}}")) {
                    var response = call("POST", "/dead-letters/" + operation, body);

                    assertJson(response, 400);
                    assertThat(response.body).as(operation + " " + body).startsWith("{\"message\":\"a blank ");
                }
            }
            assertThat(deadIds()).containsExactly(id, other);
            assertThat(outboxMessageIds()).isEmpty();
            assertThat(find("topic=&status=kept")).as("reads still take a blank one as absent")
                .containsExactly(id, other);
        });
    }

    /**
     * A typo must never widen a call: {@code eror} ignored would discard every letter of the topic, {@code kye} would
     * abandon every key. Boot's mappers ignore unknown fields.
     */
    @Test
    void unknownFieldRefused() {
        app(this::assertUnknownFieldsRefused);
    }

    private void assertUnknownFieldsRefused() throws Exception {
        long id = letter(1, "t", "k", "boom", T0, KEPT);
        seedWaiting();

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"filter\":{\"topic\":\"t\",\"eror\":\"x\"}}",
                "{\"filter\":{\"topic\":\"t\",\"kye\":\"k\"}}", "{\"filter\":{\"topic\":\"t\"},\"limit\":1}",
                "{\"idz\":[1],\"filter\":{\"topic\":\"t\"}}")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body).as(operation + " " + body).startsWith("{\"message\":\"unknown field '");
            }
        }
        var abandon = call("POST", "/waiting/abandon", "{\"topic\":\"t\",\"kye\":\"k\",\"reason\":\"r\"}");
        assertJson(abandon, 400);
        assertThat(abandon.body).isEqualTo("{\"message\":\"unknown field 'kye' in AbandonWaiting; its fields: topic,"
            + " key, reason, dryRun\"}");
        var inFilter = call("POST", "/dead-letters/discard", "{\"filter\":{\"topic\":\"t\",\"kye\":\"k\"}}");
        assertThat(inFilter.body).isEqualTo("{\"message\":\"unknown field 'kye' in Filter; its fields: topic, key,"
            + " error, failedFrom, failedTo, status\"}");
        assertThat(deadIds()).containsExactly(id);
        assertThat(outboxMessageIds()).isEmpty();
        assertThat(retryCount()).isEqualTo(4);
    }

    /**
     * Values Boot's mappers would coerce: {@code "failedTo":""} read as absent would widen a discard, {@code [1.5]}
     * would discard letter 1, {@code "dryRun":""} would move what a dry run should only count.
     */
    @Test
    void valuesAMapperWouldCoerceAreRefused() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);
            seedWaiting();

            for (var body : List.of("{\"ids\":[1.5]}", "{\"ids\":[\"1\"]}", "{\"ids\":1}", "{\"ids\":{}}",
                "{\"ids\":[9223372036854775808]}", "{\"filter\":\"t\"}", "{\"filter\":[]}",
                "{\"filter\":{\"topic\":1}}", "{\"filter\":{\"topic\":\"t\",\"key\":1}}",
                "{\"filter\":{\"topic\":\"t\",\"error\":true}}", "{\"filter\":{\"topic\":\"t\",\"failedTo\":\"\"}}",
                "{\"filter\":{\"topic\":\"t\",\"failedTo\":1}}", "{\"filter\":{\"topic\":\"t\",\"failedFrom\":\"x\"}}",
                "{\"filter\":{\"topic\":\"t\",\"status\":\"\"}}", "{\"filter\":{\"topic\":\"t\",\"status\":\"KEPT\"}}",
                "{\"filter\":{\"topic\":\"t\",\"status\":1}}")) {
                var response = call("POST", "/dead-letters/discard", body);

                assertJson(response, 400);
                assertThat(response.body).as(body).startsWith("{\"message\":\"");
            }
            for (var dryRun : List.of("\"\"", "\"true\"", "\"false\"", "1", "0")) {
                var response = call("POST", "/waiting/abandon",
                    "{\"topic\":\"t\",\"reason\":\"r\",\"dryRun\":" + dryRun + "}");

                assertJson(response, 400);
                assertThat(response.body).as(dryRun).startsWith("{\"message\":\"");
            }
            assertThat(deadIds()).containsExactly(id);
            assertThat(retryCount()).isEqualTo(4);
        });
    }

    /** An app's empty-to-null or trimming String deserializer would read these as absent, which widens the call. */
    @Test
    void blankValuesAMapperMayDropAreRefusedBeforeBinding() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);
            seedWaiting();

            for (var field : List.of("key", "failedFrom", "failedTo", "error")) {
                for (var blank : List.of("", " ")) {
                    var response = call("POST", "/dead-letters/discard",
                        "{\"filter\":{\"topic\":\"t\",\"" + field + "\":\"" + blank + "\"}}");

                    assertJson(response, 400);
                    assertThat(response.body).as(field).startsWith("{\"message\":\"a blank " + field + " in a filter");
                }
            }
            var response = call("POST", "/waiting/abandon", "{\"topic\":\"t\",\"key\":\"\",\"reason\":\"r\"}");

            assertJson(response, 400);
            assertThat(response.body).startsWith("{\"message\":\"a blank key matches");
            assertThat(deadIds()).containsExactly(id);
            assertThat(retryCount()).isEqualTo(4);
        });
    }

    /** Read as absent, a misspelt status would widen the call to every letter of the topic. */
    @Test
    void misspeltStatusIsRefusedWhenTheMapperReadsUnknownEnumsAsNull() {
        var unknownAsNull = "4.1".equals(System.getProperty("outbox.test.boot"))
            ? "spring.jackson.datatype.enum.read-unknown-enum-values-as-null=true"
            : "spring.jackson.deserialization.read-unknown-enum-values-as-null=true";
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);

            for (var operation : List.of("redrive", "discard")) {
                var response = call("POST", "/dead-letters/" + operation,
                    "{\"filter\":{\"topic\":\"t\",\"status\":\"kpet\"}}");

                assertJson(response, 400);
                assertThat(response.body).startsWith("{\"message\":\"");
            }
            assertThat(deadIds()).containsExactly(id);
            assertThat(outboxMessageIds()).isEmpty();
        }, unknownAsNull);
    }

    /** One MiB gets through, a byte more does not: 1000 ids take about 21 kB. */
    @Test
    void aBodyOverTheLimitIsRefused() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);
            var start = "{\"ids\":[" + id + "]";
            var atTheLimit = start + " ".repeat(1024 * 1024 - start.length() - 1) + "}";

            var over = call("POST", "/dead-letters/discard", " " + atTheLimit);

            assertJson(over, 400);
            assertThat(over.body)
                .isEqualTo("{\"message\":\"a request body of at most 1048576 bytes, not more\"}");
            assertThat(deadIds()).containsExactly(id);

            var at = call("POST", "/dead-letters/discard", atTheLimit);

            assertJson(at, 200);
            assertThat(at.body).isEqualTo("{\"count\":1}");
        });
    }

    @Test
    void idsAreCappedAt1000() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);
            var thousand = LongStream.rangeClosed(1, 1000).mapToObj(Long::toString).collect(Collectors.joining(","));

            var tooMany = call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + ",1001]}");

            assertJson(tooMany, 400);
            assertThat(tooMany.body).startsWith("{\"message\":\"at most 1000 ids");
            assertThat(deadIds()).containsExactly(id);
            assertThat(call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + "]}").body)
                .isEqualTo("{\"count\":1}");
        });
    }

    @Test
    void restoresAStandaloneRangeFromTheCut() {
        app(() -> {
            var orphan = foreignOrphan();
            orphanRow(orphan, 1, 10);
            var at = orphanRow(orphan, 2, 20);
            var above = orphanRow(orphan, 3, 30);

            var response = call("POST", "/orphans/" + orphan + "/restore?fromXid=20", null);

            assertJson(response, 200);
            assertThat(response.body).isEqualTo("{\"count\":2}");
            assertThat(exists(orphan)).as("dropped").isFalse();
            assertThat(outboxMessageIds()).containsExactly(at, above);
        });
    }

    @Test
    void tableNameIsNeverInterpolated() {
        app(() -> {
            var victim = TestSql.uniqueTable();
            execute("create table " + victim + " (id bigint)");
            var orphan = foreignOrphan();
            orphanRow(orphan, 1, 10);

            for (var table : List.of(orphan + ";drop table " + victim, names.base() + "_x1000' or '1'='1",
                victim, names.base(), "otherschema." + orphan)) {
                var response = call("POST", "/orphans/" + encode(table).replace("+", "%20") + "/restore?fromXid=0",
                    null);

                assertJson(response, 400);
                assertThat(response.body).as("the decoded name reached the call")
                    .startsWith("{\"message\":\"" + table + " is not ");
            }
            assertThat(exists(victim)).isTrue();
            assertThat(exists(orphan)).isTrue();
            assertThat(outboxMessageIds()).isEmpty();
        });
    }

    @Test
    void recoversAnXidRegressionOnlyWhenThereIsOne() {
        app(() -> {
            var none = call("POST", "/recovery/xid-regression", null);

            assertJson(none, 409);
            assertThat(none.body).startsWith("{\"message\":\"no xid regression to recover");

            var outbox = context.getBean(Outbox.class);
            var sent = new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                .execute(status -> outbox.sendAll(List.of(OutboxMessage.of("t", "a", bytes("1")),
                    OutboxMessage.of("t", "b", bytes("2")), OutboxMessage.of("t", "c", bytes("3")))));
            forgeRegression();

            var recovered = call("POST", "/recovery/xid-regression", null);

            assertJson(recovered, 200);
            assertThat(recovered.body).isEqualTo("{\"count\":3}");
            assertThat(outboxMessageIds()).as("re-sent with the same message ids").containsAll(sent);
            assertThat((Boolean) JsonPath.read(call("GET", "/status", null).body, "$.halted")).isFalse();
        });
    }

    @Test
    void abandonsWaitingKeysWhole() {
        app(() -> {
            seedWaiting();

            var dryRun = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"rolled back\",\"dryRun\":true}");
            assertJson(dryRun, 200);
            assertSameJson(dryRun.body, "{\"keys\":3,\"records\":5}");
            assertThat(deadIds()).as("a dry run moves nothing").isEmpty();

            var oneKey = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"key\":\"ключ\",\"reason\":\"rolled back\"}");
            assertJson(oneKey, 200);
            assertSameJson(oneKey.body, "{\"keys\":1,\"records\":1}");
            assertThat(deadIds()).as("the key's UTF-8 bytes").containsExactly(30L);

            var rest = call("POST", "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"rolled back\",\"dryRun\":false}");
            assertSameJson(rest.body, "{\"keys\":2,\"records\":4}");
            assertThat(deadIds()).containsExactly(5L, 10L, 11L, 20L, 30L);
            try (var c = connect()) {
                assertThat(TestSql.queryString(c, "select string_agg(distinct status || ' ' || error, ', ') from "
                    + names.dead())).isEqualTo("kept abandoned: rolled back");
            }
            assertThat(retryCount()).isZero();
        });
    }

    /** What the audit line of the call will carry, by name in order: the reason, unescaped; the line escapes it. */
    @Test
    void abandonLeavesItsDetailForTheAuditLine() {
        app(() -> {
            seedWaiting();
            var reason = "v2 rolled back\", dryRun \"true\r\nINFO forged";

            var result = perform("POST", API + "/waiting/abandon", "application/json",
                "{\"topic\":\"t\",\"key\":\"ключ\",\"reason\":\"v2 rolled back\\\", dryRun \\\"true\\r\\nINFO forged\"}");

            assertThat(result.getResponse().getStatus()).isEqualTo(200);
            var detail = (Map<?, ?>) result.getRequest().getAttribute(OutboxAdminController.DETAIL);
            assertThat(detail.keySet().stream().map(Object::toString))
                .containsExactly("topic", "key", "reason", "dryRun");
            assertThat(detail.get("key")).isEqualTo("ключ");
            assertThat(detail.get("reason")).isEqualTo(reason);
            assertThat(detail.get("dryRun")).isEqualTo("false");
        });
    }

    /** A typo must never widen the call: {@code kye} ignored would abandon every key of the topic. */
    @Test
    void abandonRefusesBadInput() {
        app(() -> {
            seedWaiting();

            for (var body : List.of("{\"reason\":\"r\"}", "{\"topic\":\"t\"}", "{\"topic\":\"t\",\"reason\":\" \"}",
                "{\"topic\":\"\",\"reason\":\"r\"}", "{\"topic\":\"t\",\"kye\":\"k\",\"reason\":\"r\"}",
                "{\"topic\":\"t\",\"reason\":\"r\",\"dryRun\":\"no\"}", "{\"topic\":1,\"reason\":\"r\"}", "null",
                "[]")) {
                var response = call("POST", "/waiting/abandon", body);

                assertJson(response, 400);
                assertThat(response.body).as(body).startsWith("{\"message\":\"");
            }
            assertThat(call("POST", "/waiting/abandon", "{\"reason\":\"r\"}").body)
                .isEqualTo("{\"message\":\"missing field 'topic' in AbandonWaiting\"}");
            assertThat(deadIds()).isEmpty();
        });
    }

    /** What MVC or the call refuses for its input answers 400 before anything runs. */
    @Test
    void badInputIs400() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);

            for (var query : List.of("limit=0", "limit=1001")) {
                var response = call("GET", "/dead-letters?" + query, null);

                assertJson(response, 400);
                assertThat(response.body).startsWith("{\"message\":\"limit must be within 1..1000");
            }
            var badLimit = call("GET", "/dead-letters?limit=x", null);
            assertJson(badLimit, 400);
            assertThat(badLimit.body).isEqualTo("{\"message\":\"Query parameter 'limit' has invalid value: x\"}");
            var malformed = call("POST", "/dead-letters/discard", "not json");
            assertJson(malformed, 400);
            assertThat(malformed.body).startsWith("{\"message\":\"Unrecognized token 'not'");

            var rejected = new ArrayList<Response>();
            rejected.add(call("GET", "/dead-letters?afterId=x", null));
            for (var query : List.of("failedFrom=yesterday", "failedTo=2026-10-27", "status=PENDING", "status=gone")) {
                rejected.add(call("GET", "/dead-letters?" + query, null));
                rejected.add(call("GET", "/dead-letters/count?" + query, null));
            }
            rejected.add(call("POST", "/orphans/" + names.base() + "_x1000/restore", null)); // fromXid required
            rejected.add(call("POST", "/orphans/" + names.base() + "_x1000/restore?fromXid=x", null));
            rejected.add(call("POST", "/orphans/" + names.base() + "_x1000/restore?fromXid=", null));
            rejected.add(call("POST", "/dead-letters/redrive", "{\"ids\":[\"x\"]}"));
            rejected.add(call("POST", "/dead-letters/redrive", null)); // no body, no Content-Type
            rejected.add(call("POST", "/dead-letters/discard", null));
            rejected.add(call("POST", "/waiting/abandon", null));
            for (var response : rejected) {
                assertJson(response, 400);
                assertThat(response.body).as(response.uri).startsWith("{\"message\":\"");
            }
            assertThat(deadIds()).containsExactly(id);
        });
    }

    /** Spring MVC reads JSON only by its Content-Type; a form post, which a page elsewhere can send, is no JSON. */
    @Test
    void anotherContentTypeIs415() {
        app(() -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);

            for (var type : List.of("text/plain", "application/x-www-form-urlencoded")) {
                var result = perform("POST", API + "/dead-letters/discard", type, "{\"ids\":[" + id + "]}");

                assertThat(result.getResponse().getStatus()).isEqualTo(415);
                assertThat(result.getResponse().getContentType()).startsWith("application/json");
                assertThat(result.getResponse().getContentAsString(UTF_8)).startsWith("{\"message\":\"");
            }
            assertThat(deadIds()).containsExactly(id);
        });
    }

    /**
     * Neither bad input nor a refusal of the outbox's state: a 500 whose {@code Error} has the failure's message, for
     * the authenticated operator; one WARN with the exception.
     */
    @Test
    void otherFailuresAreA500Error() {
        app(() -> {
            execute("drop table " + names.dead() + " cascade");

            Response response;
            String log;
            try (var stderr = new CapturedStderr()) {
                response = call("GET", "/dead-letters/count", null);
                log = stderr.text();
            }

            assertJson(response, 500);
            assertThat(response.body).startsWith("{\"message\":\"").contains(names.dead() + "\\\" does not exist");
            var call = "outbox admin GET \"" + API + "/dead-letters/count\"";
            assertThat(log.lines().filter(l -> l.contains(" WARN ") && l.contains(call + " failed, answered 500")))
                .as("one WARN").hasSize(1);
            assertThat(log).contains("PSQLException");
        });
    }

    /** The JSON the contract names, whatever the app configured its mapper to do. */
    @Test
    void theAppsMapperSettingsDoNotChangeTheJson() {
        app(() -> {
            long id = letter(7, "t", "k", "timeout", T0, PENDING);
            execute("update " + names.dead() + " set redrives = 2, handler_attempts = 1 where id = " + id);

            var response = call("GET", "/dead-letters?topic=t", null);

            assertJson(response, 200);
            assertSameJson(response.body, letterJson(id, messageId(id)));
            assertThat(call("POST", "/dead-letters/discard", "{\"filter\":{\"topic\":\"t\",\"failedTo\":"
                + "\"2026-01-15T10:00:00.001Z\"}}").body).isEqualTo("{\"count\":1}");
        }, "spring.jackson.property-naming-strategy=SNAKE_CASE", "spring.jackson.default-property-inclusion=always",
            "spring.jackson.time-zone=Asia/Tokyo", "spring.jackson.date-format=yyyy");
    }

    /** Boot 4.1 can keep MVC on Jackson 2, as an app in the middle of its migration does. */
    @Test
    void jackson2OnBoot41() {
        assumeTrue("4.1".equals(System.getProperty("outbox.test.boot")));
        app(() -> {
            assertThat(context.getBean(RequestMappingHandlerAdapter.class).getMessageConverters())
                .anyMatch(c -> c.getClass().getName().endsWith("MappingJackson2HttpMessageConverter"));
            assertUnknownFieldsRefused();
            execute("update " + names.dead() + " set redrives = 2, handler_attempts = 1");
            assertThat(JsonPath.<List<String>>read(call("GET", "/dead-letters", null).body, "$.items[*].failedAt"))
                .containsExactly("2026-01-15T10:00:00.000Z");
        }, "spring.http.converters.preferred-json-mapper=jackson2");
    }

    @RestController
    static class AppController {

        record Body(String name) {}

        @GetMapping("/app/fail")
        String fail() {
            throw new IllegalArgumentException("the app's own failure");
        }

        @PostMapping("/app/echo")
        String echo(@RequestBody Body body) {
            return body.name();
        }
    }

    @RestControllerAdvice
    static class AppAdvice {

        @ExceptionHandler(Exception.class)
        ResponseEntity<String> any(Exception e) {
            return ResponseEntity.status(418).body("the app's handler: " + e.getClass().getSimpleName());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class App {

        @Bean
        AppController appController() {
            return new AppController();
        }

        @Bean
        AppAdvice appAdvice() {
            return new AppAdvice();
        }
    }

    /** Each one's failures stay its own, its lenient bodies too, Boot's problem details on or off. */
    @Test
    void appControllerAndItsErrorHandlingAreUntouched() {
        for (var problemDetails : List.of("false", "true")) {
            app(new Class<?>[] {App.class}, () -> {
                assertThat(perform("GET", "/app/fail", null, null).getResponse().getStatus()).isEqualTo(418);
                assertThat(perform("POST", "/app/echo", "application/json", "{\"name\":\"n\",\"extra\":1}")
                    .getResponse().getContentAsString()).isEqualTo("n");

                var refused = call("POST", "/dead-letters/discard", "{\"idz\":[1]}");
                assertJson(refused, 400);
                assertThat(refused.body).startsWith("{\"message\":\"unknown field 'idz'");
                var malformed = call("POST", "/dead-letters/discard", "not json");
                assertJson(malformed, 400);
                assertThat(malformed.body).startsWith("{\"message\":\"Unrecognized token 'not'");
                var badLimit = call("GET", "/dead-letters?limit=x", null);
                assertJson(badLimit, 400);
                assertThat(badLimit.body).startsWith("{\"message\":\"Query parameter 'limit'");
                execute("drop table " + names.dead() + " cascade");
                assertJson(call("GET", "/dead-letters/count", null), 500);
            }, "spring.mvc.problemdetails.enabled=" + problemDetails);
        }
    }

    /** What many apps have: first in order, answering every exception in the app's own way. */
    @RestControllerAdvice
    @Order(Ordered.HIGHEST_PRECEDENCE)
    static class FirstAppAdvice {

        @ExceptionHandler(Exception.class)
        ResponseEntity<String> any(Exception e) {
            return ResponseEntity.status(418).body("the app's first handler: " + e.getClass().getSimpleName());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FirstApp {

        @Bean
        AppController appController() {
            return new AppController();
        }

        @Bean
        FirstAppAdvice firstAppAdvice() {
            return new FirstAppAdvice();
        }
    }

    @Test
    void anAppsFirstInOrderAdviceDoesNotAnswerForTheAdmin() {
        app(new Class<?>[] {FirstApp.class}, () -> {
            assertThat(perform("GET", "/app/fail", null, null).getResponse().getStatus()).isEqualTo(418);

            var refused = call("POST", "/dead-letters/discard", "{\"idz\":[1]}");
            assertJson(refused, 400);
            assertThat(refused.body).startsWith("{\"message\":\"unknown field 'idz'");
            var conflict = call("POST", "/recovery/xid-regression", null);
            assertJson(conflict, 409);
            assertThat(conflict.body).startsWith("{\"message\":\"no xid regression to recover");
            execute("drop table " + names.dead() + " cascade");
            var failed = call("GET", "/dead-letters/count", null);
            assertJson(failed, 500);
            assertThat(failed.body).contains(names.dead() + "\\\" does not exist");
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class RefusingInterceptor implements WebMvcConfigurer {

        @Override
        public void addInterceptors(InterceptorRegistry registry) {
            registry.addInterceptor(new HandlerInterceptor() {
                @Override
                public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                    throw new IllegalArgumentException("refused before the method");
                }
            }).addPathPatterns(API + "/**");
        }
    }

    /** As a guard's failure: the route's method has not run, the controller's own handlers still answer. */
    @Test
    void anExceptionBeforeTheMethodRunsGetsAnError() {
        app(new Class<?>[] {FirstApp.class, RefusingInterceptor.class}, () -> {
            long id = letter(1, "t", "k", "boom", T0, KEPT);

            var refused = call("POST", "/dead-letters/discard", "{\"ids\":[" + id + "]}");

            assertJson(refused, 400);
            assertThat(refused.body).isEqualTo("{\"message\":\"refused before the method\"}");
            assertThat(deadIds()).containsExactly(id);
        });
    }

    @Aspect
    static class EveryController {

        @Around("@within(org.springframework.web.bind.annotation.RestController)")
        Object around(ProceedingJoinPoint call) throws Throwable {
            return call.proceed();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithAspect {

        @Bean
        EveryController everyController() {
            return new EveryController();
        }
    }

    /** An app's aspect over every controller, a logging one say, proxies this one too. */
    @Test
    void worksBehindAnAppsAspect() {
        app(new Class<?>[] {WithAspect.class}, () -> {
            assertThat(context.getBean(OutboxAdminController.class).getClass().getName()).contains("$$");
            letter(1, "t", "k", "boom", T0, KEPT);

            assertThat(call("GET", "/dead-letters/count", null).body).isEqualTo("{\"count\":1}");
            assertThat(call("POST", "/dead-letters/discard", "{\"idz\":[1]}").body)
                .startsWith("{\"message\":\"unknown");
        });
    }

    @Test
    void noControllerWithoutTheOutboxOrServletMvc() {
        app(() -> assertThat(context.getBeansOfType(OutboxAdminController.class)).hasSize(1));
        new WebApplicationContextRunner()
            .withUserConfiguration(Boot.class).withPropertyValues(properties("outbox.enabled=false"))
            .run(c -> assertThat(c).hasNotFailed().doesNotHaveBean(OutboxAdminController.class));
        new ApplicationContextRunner().withUserConfiguration(Boot.class).withPropertyValues(properties())
            .run(c -> assertThat(c).hasNotFailed().doesNotHaveBean(OutboxAdminController.class));
        new WebApplicationContextRunner()
            .withClassLoader(new FilteredClassLoader(RequestMappingHandlerAdapter.class))
            .withUserConfiguration(Boot.class).withPropertyValues(properties())
            .run(c -> assertThat(c).hasNotFailed().doesNotHaveBean(OutboxAdminController.class));
    }

    // ---------------------------------------------------------------------------------------------------- fixtures

    /**
     * A dead letter as the relay writes one: payload p, headers h1=v and h2 without a value, created an hour before
     * {@code failedAt}; a pending or scheduled one is due an hour after it.
     */
    private long letter(long id, String topic, String key, String error, Instant failedAt, DeadLetter.Status status)
        throws SQLException {
        var headers = HeaderCodec.encode(List.of(new OutboxHeader("h1", bytes("v")), new OutboxHeader("h2", null)));
        try (var c = connect(); var ps = c.prepareStatement("insert into " + names.dead()
            + " (id, shard, message_id, topic, key, payload, headers, created_at, attempts, failed_at, error,"
            + " status, due_at) values (?, 0, gen_random_uuid(), ?, ?, ?, ?, ?, 3, ?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, topic);
            ps.setBytes(3, bytes(key));
            ps.setBytes(4, bytes("p"));
            ps.setBytes(5, headers);
            ps.setObject(6, failedAt.minusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.setObject(7, failedAt.atOffset(ZoneOffset.UTC));
            ps.setString(8, error);
            ps.setString(9, status.name().toLowerCase(Locale.ROOT));
            ps.setObject(10, status == KEPT ? null : failedAt.plusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        }
        return id;
    }

    /**
     * Topic t, shard 0: waiting head 10 of key k with follower 11 and parked row 5; unkeyed waiting head 20; waiting
     * head 30 of key ключ.
     */
    private void seedWaiting() throws SQLException {
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, next_attempt_at,"
            + " waiting_since) values (10, 0, gen_random_uuid(), 't', 'k', now(), now(), now()),"
            + " (11, 0, gen_random_uuid(), 't', 'k', now(), null, null),"
            + " (20, 0, gen_random_uuid(), 't', null, now(), now(), now()),"
            + " (30, 0, gen_random_uuid(), 't', convert_to('ключ', 'UTF8'), now(), now(), now())");
        execute("insert into " + names.parked() + " (id, shard, message_id, topic, key, created_at, due_at)"
            + " values (5, 0, gen_random_uuid(), 't', 'k', now(), now())");
    }

    private long retryCount() throws SQLException {
        try (var c = connect()) {
            return TestSql.queryLong(c, "select count(*) from " + names.retry());
        }
    }

    private UUID messageId(long letter) throws SQLException {
        try (var c = connect()) {
            return UUID.fromString(TestSql.queryString(c, "select message_id::text from " + names.dead()
                + " where id = " + letter));
        }
    }

    /** A standalone {@code <base>_x1000}, below every range of the outbox, as someone else's detach leaves one. */
    private String foreignOrphan() throws SQLException {
        var orphan = names.partitionPrefix() + "1000";
        execute("create table " + orphan + " (like " + names.base() + " including defaults)");
        return orphan;
    }

    /** A row of a standalone range with that xid; returns its message id. */
    private UUID orphanRow(String table, long id, long xid) throws SQLException {
        var messageId = UUID.randomUUID();
        try (var c = connect(); var ps = c.prepareStatement("insert into " + table
            + " (id, shard, xid, message_id, topic, key, payload) values (?, 0, ?::text::xid8, ?, 't', ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, Long.toString(xid));
            ps.setObject(3, messageId);
            ps.setBytes(4, bytes("k" + id));
            ps.setBytes(5, bytes("row"));
            ps.executeUpdate();
        }
        return messageId;
    }

    /** As after a dump/restore: a tick whose snapshot xmax lies 10⁹ xids past the counter, made a minute ago. */
    private void forgeRegression() throws SQLException {
        try (var c = connect()) {
            long xmax = TestSql.queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint")
                + 1_000_000_000L;
            TestSql.execute(c, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                + ":'::pg_snapshot, now() - interval '1 minute')");
        }
    }

    private List<Long> deadIds() throws SQLException {
        var all = new ArrayList<Long>();
        try (var c = connect(); var st = c.createStatement();
            var rs = st.executeQuery("select id from " + names.dead() + " order by id")) {
            while (rs.next()) {
                all.add(rs.getLong(1));
            }
        }
        return all;
    }

    private List<UUID> outboxMessageIds() throws SQLException {
        var all = new ArrayList<UUID>();
        try (var c = connect(); var st = c.createStatement();
            var rs = st.executeQuery("select message_id from " + names.base() + " order by id")) {
            while (rs.next()) {
                all.add(rs.getObject(1, UUID.class));
            }
        }
        return all;
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = connect()) {
            return TestSql.queryString(c, "select to_regclass(?)::text", table) != null;
        }
    }

    private static void execute(String sql) throws SQLException {
        try (var c = connect()) {
            TestSql.execute(c, sql);
        }
    }

    private static Connection connect() throws SQLException {
        return TestDataSources.newDataSource().getConnection();
    }

    // ------------------------------------------------------------------------------------------------------- HTTP

    private record Response(String uri, int status, String contentType, String body) {}

    private List<Long> find(String query) throws Exception {
        var response = call("GET", "/dead-letters?" + query, null);
        assertJson(response, 200);
        return ids(response.body);
    }

    private static List<Long> ids(String page) {
        return JsonPath.<List<Number>>read(page, "$.items[*].id").stream().map(Number::longValue).toList();
    }

    private Response call(String method, String path, String body) throws Exception {
        var uri = API + path;
        var response = perform(method, uri, body == null ? null : "application/json", body).getResponse();
        return new Response(uri, response.getStatus(), response.getContentType(), response.getContentAsString(UTF_8));
    }

    /**
     * Sets the body on the built request: the builder's own setters, compiled against Spring 6, are not there on
     * Spring 7, which moved them to a generic superclass.
     */
    private MvcResult perform(String method, String uri, String contentType, String body) throws Exception {
        var builder = MockMvcRequestBuilders.request(HttpMethod.valueOf(method), URI.create(uri));
        return mvc.perform(servletContext -> {
            var request = builder.buildRequest(servletContext);
            if (body != null) {
                request.setContentType(contentType);
                request.setCharacterEncoding("UTF-8");
                request.setContent(bytes(body));
            }
            return request;
        }).andReturn();
    }

    private static void assertJson(Response response, int status) {
        assertThat(response.status).as(response.body).isEqualTo(status);
        assertThat(response.contentType).startsWith("application/json");
    }

    /** Field order aside (Jackson 3 sorts by default), and nothing extra. */
    private static void assertSameJson(String actual, String expected) throws JSONException {
        JSONAssert.assertEquals(expected, actual, true);
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
}
