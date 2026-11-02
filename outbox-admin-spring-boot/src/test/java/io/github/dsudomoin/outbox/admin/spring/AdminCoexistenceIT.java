package io.github.dsudomoin.outbox.admin.spring;

import static io.github.dsudomoin.outbox.admin.spring.AdminServer.API;
import static io.github.dsudomoin.outbox.admin.spring.AdminServer.assertError;
import static io.github.dsudomoin.outbox.admin.spring.AdminServer.awaitAudit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.admin.spring.AdminServer.Keys;
import io.github.dsudomoin.outbox.admin.spring.AdminServer.WithKeys;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The admin API in an app with its own routes (two at the admin API's names without its prefix), its own filter,
 * interceptor and error handling, behind the app's API-key authorizer. The relay is disabled, so only calls change the
 * outbox. Each test has its own outbox table and server.
 */
class AdminCoexistenceIT {

    private static final String STATUS = API + "/status";
    private static final String KEY = "X-Api-Key";
    private static final String APP_KEY = "X-App-Key";

    /** An admin route: its operation, method, path ({@code {orphan}} is the test's orphan table) and body. */
    record Route(String operation, String method, String path, String body) {

        @Override
        public String toString() {
            return operation;
        }
    }

    /** All eight, in the contract's order, each acting on what {@link #seed} made when allowed. */
    static final List<Route> ROUTES = List.of(
        new Route("getStatus", "GET", API + "/status", null),
        new Route("findDeadLetters", "GET", API + "/dead-letters?topic=t", null),
        new Route("countDeadLetters", "GET", API + "/dead-letters/count?topic=t", null),
        new Route("redriveDeadLetters", "POST", API + "/dead-letters/redrive", "{\"ids\":[1]}"),
        new Route("discardDeadLetters", "POST", API + "/dead-letters/discard", "{\"ids\":[1]}"),
        new Route("restoreOrphan", "POST", API + "/orphans/{orphan}/restore?fromXid=0", null),
        new Route("recoverXidRegression", "POST", API + "/recovery/xid-regression", null),
        new Route("abandonWaiting", "POST", API + "/waiting/abandon", "{\"topic\":\"t\",\"reason\":\"test\"}"));

    /** Its own routes, its own key, the interceptor that checks it, and its own way of answering a failure. */
    @RestController
    static class AppController {

        @GetMapping("/hello")
        String hello() {
            return "hello";
        }

        @GetMapping("/status")
        String status() {
            return "app status";
        }

        @GetMapping("/dead-letters")
        String deadLetters() {
            return "app dead letters";
        }

        @GetMapping("/app/fail")
        String fail() {
            throw new IllegalArgumentException("the app's own failure");
        }
    }

    /** What many apps have: first in order, answering every exception in the app's own way. */
    @RestControllerAdvice
    @Order(Ordered.HIGHEST_PRECEDENCE)
    static class AppAdvice {

        @ExceptionHandler(Exception.class)
        ResponseEntity<String> any(Exception e) {
            return ResponseEntity.status(418).body("the app's handler: " + e.getClass().getSimpleName());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class App implements WebMvcConfigurer {

        private final List<String> intercepted = new CopyOnWriteArrayList<>();

        @Bean
        AppController appController() {
            return new AppController();
        }

        @Bean
        AppAdvice appAdvice() {
            return new AppAdvice();
        }

        @Bean
        Filter appFilter() {
            return (request, response, chain) -> {
                ((HttpServletResponse) response).setHeader("X-Test-Global", "1");
                chain.doFilter(request, response);
            };
        }

        /** The paths its interceptor saw, for the test. */
        @Bean
        List<String> intercepted() {
            return intercepted;
        }

        @Override
        public void addInterceptors(InterceptorRegistry registry) {
            registry.addInterceptor(new HandlerInterceptor() {
                @Override
                public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                    intercepted.add(request.getRequestURI());
                    return true;
                }
            });
            registry.addInterceptor(new HandlerInterceptor() {
                @Override
                public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                    if ("app-key".equals(request.getHeader(APP_KEY))) {
                        return true;
                    }
                    response.setStatus(401);
                    return false;
                }
            }).addPathPatterns("/status", "/dead-letters");
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> intercepted(AdminServer server) {
        return server.context.getBean("intercepted", List.class);
    }

    private static AdminServer app() {
        return AdminServer.start(WithKeys.class, App.class);
    }

    static Stream<Route> routes() {
        return ROUTES.stream();
    }

    /** The list here is what {@link #seed} covers; the guard test goes through the app's mappings themselves. */
    @Test
    void theRoutesHereAreAllOfTheControllers() {
        try (var server = app()) {
            var operations = ROUTES.stream().map(Route::operation).toList();
            assertThat(server.routes().stream().map(AdminServer.Route::handler).filter(h -> !h.equals("notJson"))
                .toList()).containsExactlyInAnyOrderElementsOf(operations);
        }
    }

    @ParameterizedTest(name = "{0} answers 401 and 403")
    @MethodSource("routes")
    void everyAdminRouteAnswers401And403(Route route) throws Exception {
        try (var server = app()) {
            seed(server);
            var before = state(server);
            HttpResponse<String> noKey;
            HttpResponse<String> wrongKey;
            List<String> lines;
            try (var stderr = new CapturedStderr()) {
                noKey = call(server, route, null);
                wrongKey = call(server, route, "wrong");
                lines = server.settledAudit(stderr, 2);
            }

            assertError(noKey, 401, "X-Api-Key required");
            assertError(wrongKey, 403, "unknown key");
            assertThat(state(server)).as("dead letters, outbox rows, the orphan and the ticks unchanged")
                .isEqualTo(before);
            assertThat(lines).as("one INFO line per call").containsExactly(
                "outbox admin " + route.method() + " \"" + path(server, route) + "\" denied: 401",
                "outbox admin " + route.method() + " \"" + path(server, route) + "\" denied: 403");
            assertThat(call(server, route, "secret").statusCode()).isEqualTo(200);
            if (route.method().equals("POST")) {
                assertThat(state(server)).as("what an allowed call changes, so the check above can fail")
                    .isNotEqualTo(before);
            }
        }
    }

    /** A filter sits outside the guard, as Kora's global interceptor does; the app's interceptors within. */
    @Test
    void theAppsFilterAppliesToAdminRoutesAndItsInterceptorToTheAllowedOnes() throws Exception {
        try (var server = app()) {
            seed(server);

            for (var route : ROUTES) {
                for (var key : new String[] {null, "wrong", "secret"}) {
                    assertThat(call(server, route, key).headers().firstValue("X-Test-Global"))
                        .as(route + " with key " + key).hasValue("1");
                }
            }

            assertThat(intercepted(server)).as("only the allowed calls got past the guard")
                .containsExactlyElementsOf(ROUTES.stream().map(r -> path(server, r)).toList());
        }
    }

    /** The app's routes, its key and its interceptor, beside ours: neither guards the other. */
    @Test
    void ownRoutesAndErrorHandlingStillWork() throws Exception {
        try (var server = app()) {
            var hello = server.get("/hello");
            var status = server.get("/status", APP_KEY, "app-key");
            var deadLetters = server.get("/dead-letters", APP_KEY, "app-key");

            for (var response : List.of(hello, status, deadLetters)) {
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(response.headers().firstValue("X-Test-Global")).hasValue("1");
            }
            assertThat(hello.body()).isEqualTo("hello");
            assertThat(status.body()).isEqualTo("app status");
            assertThat(deadLetters.body()).isEqualTo("app dead letters");
            for (var path : List.of("/status", "/dead-letters")) {
                assertThat(server.get(path).statusCode()).as("the app's own check runs on " + path).isEqualTo(401);
                assertThat(server.get(path, KEY, "secret").statusCode()).as("the admin key opens no app route")
                    .isEqualTo(401);
            }
            var failed = server.get("/app/fail");
            assertThat(failed.statusCode()).isEqualTo(418);
            assertThat(failed.body()).isEqualTo("the app's handler: IllegalArgumentException");
            assertThat(server.send("POST", "/app/fail", "x", "Sec-Fetch-Site", "cross-site").statusCode())
                .as("no cross-site refusal off the admin routes, MVC's 405 by way of the app's handler").isEqualTo(418);
            assertThat(server.bean(Keys.class).threads).as("the admin authorizer guards the admin routes alone")
                .isEmpty();

            var adminStatus = server.get(STATUS, KEY, "secret");
            assertThat(adminStatus.statusCode()).as("no app key needed").isEqualTo(200);
            assertThat(adminStatus.body()).contains("\"halted\":false");
        }
    }

    /** The app's advice answers every failure of the app's; a denial stays the guard's, whatever its order. */
    @Test
    void anAppsFirstInOrderAdviceDoesNotTakeOverADenial() throws Exception {
        try (var server = app()) {
            assertError(server.get(STATUS), 401, "X-Api-Key required");
            assertError(server.get(STATUS, KEY, "wrong"), 403, "unknown key");
            assertError(server.post(API + "/dead-letters/discard", "{}", "Sec-Fetch-Site", "cross-site"), 403,
                "cross-site request refused");

            server.bean(Keys.class).answer = request -> {
                throw new IllegalStateException("key store down");
            };

            assertError(server.get(STATUS), 500, "authorization failed");
        }
    }

    @Test
    void auditLineNamesTheIdentity() throws Exception {
        try (var server = app(); var stderr = new CapturedStderr()) {
            seed(server);
            int lines = 0;

            for (var route : ROUTES) {
                call(server, route, null);
                call(server, route, "wrong");
                assertThat(call(server, route, "secret").statusCode()).as(route.toString()).isEqualTo(200);
                lines += 3;
                awaitAudit(stderr, lines);
            }

            var expected = new ArrayList<String>();
            for (var route : ROUTES) {
                var call = "outbox admin " + route.method() + " \"" + path(server, route) + "\"";
                var allowed = call + " by \"tester\"" + (route.operation().equals("abandonWaiting")
                    ? " with topic \"t\", key null, reason \"test\", dryRun \"false\"" : "") + ": 200";
                expected.addAll(List.of(call + " denied: 401", call + " denied: 403", allowed));
            }
            assertThat(server.settledAudit(stderr, lines)).as("one INFO line per call")
                .containsExactlyElementsOf(expected);
        }
    }

    // ---------------------------------------------------------------------------------------------------- fixtures

    /**
     * Something for every mutating route to change: kept dead letter 1, the foreign orphan {@code <table>_x1000} with
     * one row, one outbox row, a tick whose snapshot xmax lies 10⁹ xids past the counter, as after a dump/restore, and
     * waiting retry head 2.
     */
    private static void seed(AdminServer server) throws SQLException {
        var names = server.names;
        server.execute("insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
            + " values (1, 0, gen_random_uuid(), 't', now(), 3)");
        server.execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at, next_attempt_at,"
            + " waiting_since) values (2, 0, gen_random_uuid(), 't', now(), now(), now())");
        server.execute("create table " + orphan(server) + " (like " + names.base() + " including defaults)");
        server.execute("insert into " + orphan(server) + " (id, shard, xid, message_id, topic)"
            + " values (1, 0, '10', gen_random_uuid(), 't')");
        server.execute("insert into " + names.base() + " (shard, message_id, topic)"
            + " values (0, gen_random_uuid(), 't')");
        server.execute("insert into " + names.tick() + " (snapshot, created_at)"
            + " select (x || ':' || x || ':')::pg_snapshot, now() - interval '1 minute'"
            + " from (select pg_snapshot_xmax(pg_current_snapshot())::text::bigint"
            + " + 1000000000 x) s");
    }

    /** What the mutating routes change: dead letter ids, outbox message ids, the orphan's rows, the ticks. */
    private static String state(AdminServer server) throws SQLException {
        var names = server.names;
        try (var c = server.bean(DataSource.class).getConnection()) {
            var orphanRows = TestSql.queryString(c, "select to_regclass(?)::text", orphan(server)) == null ? "dropped"
                : TestSql.queryString(c, "select count(*)::text from " + orphan(server));
            return String.join(" | ",
                TestSql.queryString(c, "select string_agg(id::text, ',' order by id) from " + names.dead()),
                TestSql.queryString(c, "select string_agg(message_id::text, ',' order by id) from " + names.base()),
                orphanRows,
                TestSql.queryString(c, "select string_agg(id::text, ',' order by id) from " + names.tick()));
        }
    }

    private static String orphan(AdminServer server) {
        return server.names.partitionPrefix() + "1000";
    }

    /** {@code route}'s path without its query, as the audit line names it. */
    private static String path(AdminServer server, Route route) {
        var path = route.path().replace("{orphan}", orphan(server));
        return path.contains("?") ? path.substring(0, path.indexOf('?')) : path;
    }

    private static HttpResponse<String> call(AdminServer server, Route route, String apiKey) throws Exception {
        var uri = route.path().replace("{orphan}", orphan(server));
        var headers = new ArrayList<String>();
        if (route.body() != null) {
            headers.addAll(List.of("Content-Type", "application/json"));
        }
        if (apiKey != null) {
            headers.addAll(List.of(KEY, apiKey));
        }
        return server.send(route.method(), uri, route.body(), headers.toArray(String[]::new));
    }
}
