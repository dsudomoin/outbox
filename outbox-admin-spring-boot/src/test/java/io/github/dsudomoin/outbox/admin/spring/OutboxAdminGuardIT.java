package io.github.dsudomoin.outbox.admin.spring;

import static io.github.dsudomoin.outbox.admin.spring.AdminServer.API;
import static io.github.dsudomoin.outbox.admin.spring.AdminServer.assertError;
import static io.github.dsudomoin.outbox.admin.spring.AdminServer.audit;
import static io.github.dsudomoin.outbox.admin.spring.AdminServer.awaitAudit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.admin.spring.AdminServer.Keys;
import io.github.dsudomoin.outbox.admin.spring.AdminServer.WithKeys;
import io.github.dsudomoin.outbox.admin.spring.OutboxAdminAuthorizer.Allow;
import io.github.dsudomoin.outbox.admin.spring.OutboxAdminAuthorizer.Deny;
import io.github.dsudomoin.outbox.admin.spring.api.OutboxAdminApi;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.handler.MappedInterceptor;

/**
 * Who gets to the admin routes, on a real Tomcat. The {@link Spy} sees which handler each call was mapped to, ahead of
 * the guard, and which methods of the controller then ran.
 */
class OutboxAdminGuardIT {

    private static final String STATUS = API + "/status";
    private static final String DISCARD = API + "/dead-letters/discard";
    private static final String KEY = "X-Api-Key";

    @Aspect
    static class Spy {

        final List<String> mapped = new CopyOnWriteArrayList<>();
        final List<String> ran = new CopyOnWriteArrayList<>();
        final List<String> threads = new CopyOnWriteArrayList<>();

        @Around("within(io.github.dsudomoin.outbox.admin.spring.OutboxAdminController)")
        Object around(ProceedingJoinPoint call) throws Throwable {
            if (!call.getSignature().getName().equals("denied")) { // the guard's answer, not a route
                ran.add(call.getSignature().getName());
            }
            return call.proceed();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithSpy {

        @Bean
        Spy spy() {
            return new Spy();
        }

        /** A bean of the user's configuration, so ahead of the auto-configured guard. */
        @Bean
        MappedInterceptor spyInterceptor(Spy spy) {
            return new MappedInterceptor(null, new HandlerInterceptor() {
                @Override
                public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                    if (handler instanceof HandlerMethod method) {
                        spy.mapped.add(method.getMethod().getName());
                        spy.threads.add(Thread.currentThread().getName());
                    }
                    return true;
                }
            });
        }
    }

    /** What {@code addInterceptors} would not reach: Boot's MVC configuration has backed off. */
    @Configuration(proxyBeanMethods = false)
    static class ExtendsSupport extends WebMvcConfigurationSupport {}

    /** The routes move; a guard by path would miss them. */
    @Configuration(proxyBeanMethods = false)
    static class MovesTheRoutes implements WebMvcConfigurer {

        @Override
        public void configurePathMatch(PathMatchConfigurer configurer) {
            configurer.addPathPrefix("/api", HandlerTypePredicate.forAnnotation(RestController.class));
        }
    }

    static Stream<Class<?>> apps() {
        return Stream.of(WithSpy.class, ExtendsSupport.class, MovesTheRoutes.class);
    }

    /** From the app's own mappings, so a route added later is in here by itself. */
    @ParameterizedTest
    @MethodSource("apps")
    void everyRouteAnswers401WithNoAuthorizerPassAndDoesNotRun(Class<?> app) throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class, app)) {
            var spy = server.bean(Spy.class);
            var routes = server.routes();
            assertThat(routes.stream().map(AdminServer.Route::handler)).as("the contract's and the extra one")
                .contains("getStatus", "findDeadLetters", "countDeadLetters", "redriveDeadLetters",
                    "discardDeadLetters", "restoreOrphan", "recoverXidRegression", "abandonWaiting", "notJson");
            assertThat(routes.stream().filter(r -> r.path().endsWith("/orphans/x/restore"))).isNotEmpty();
            assertThat(routes).as("eight of the contract and three of notJson").hasSizeGreaterThanOrEqualTo(11);
            if (app == MovesTheRoutes.class) {
                assertThat(routes).allSatisfy(r -> assertThat(r.path()).startsWith("/api/"));
            }

            for (var route : routes) {
                spy.mapped.clear();
                var noKey = route.contentType() == null ? server.send(route.method(), route.path(), null)
                    : server.send(route.method(), route.path(), "not json", "Content-Type", route.contentType());

                assertError(noKey, 401, "X-Api-Key required");
                assertThat(spy.mapped).as("the call got to " + route).containsExactly(route.handler());
            }
            assertThat(spy.ran).as("no route ran").isEmpty();

            var allowed = routes.stream().filter(r -> r.handler().equals("getStatus")).findFirst().orElseThrow();
            assertThat(server.get(allowed.path(), KEY, "secret").statusCode()).as("so the above can fail")
                .isEqualTo(200);
            assertThat(spy.ran).contains("getStatus");
        }
    }

    /**
     * An aspect with {@code spring.aop.proxy-target-class=false} makes the controller a JDK proxy, which MVC cannot
     * invoke the class's methods on, its exception handlers' included: every call fails, with a key or without, and
     * none acts.
     */
    @Test
    void aJdkProxyOfTheControllerRunsNoRoute() throws Exception {
        try (var server = AdminServer.start(List.of("spring.aop.proxy-target-class=false"), WithKeys.class,
            WithSpy.class)) {
            assertThat(Proxy.isProxyClass(server.bean(OutboxAdminApi.class).getClass())).isTrue();
            server.execute("insert into " + server.names.dead()
                + " (id, shard, message_id, topic, created_at, attempts)"
                + " values (1, 0, gen_random_uuid(), 't', now(), 3)");

            assertThat(server.post(DISCARD, "{\"ids\":[1]}").statusCode()).isEqualTo(500);
            assertThat(server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret").statusCode()).isEqualTo(500);

            try (var c = server.bean(DataSource.class).getConnection()) {
                assertThat(TestSql.queryLong(c, "select count(*) from " + server.names.dead())).isEqualTo(1);
            }
        }
    }

    /** MVC answers an OPTIONS by itself, with its own handler: the methods of the path, no route, no guard. */
    @Test
    void anOptionsCallRunsNoRoute() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class)) {
            var options = server.send("OPTIONS", DISCARD, null);

            assertThat(options.statusCode()).isEqualTo(200);
            assertThat(options.body()).isEmpty();
            assertThat(server.bean(Spy.class).ran).isEmpty();
            assertThat(server.bean(Keys.class).threads).isEmpty();
        }
    }

    static class BodyProbe extends OncePerRequestFilter {

        final AtomicInteger reads = new AtomicInteger();

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
            chain.doFilter(new HttpServletRequestWrapper(request) {
                @Override
                public ServletInputStream getInputStream() throws IOException {
                    reads.incrementAndGet();
                    return super.getInputStream();
                }

                @Override
                public BufferedReader getReader() throws IOException {
                    reads.incrementAndGet();
                    return super.getReader();
                }
            }, response);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithBodyProbe {

        @Bean
        BodyProbe bodyProbe() {
            return new BodyProbe();
        }
    }

    @Test
    void aDeniedPostDoesNotReadItsBodyAndChangesNothing() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class, WithBodyProbe.class)) {
            var probe = server.bean(BodyProbe.class);
            server.execute("insert into " + server.names.dead()
                + " (id, shard, message_id, topic, created_at, attempts)"
                + " values (1, 0, gen_random_uuid(), 't', now(), 3)");

            assertError(server.post(DISCARD, "{\"ids\":[1]}"), 401, "X-Api-Key required");
            assertError(server.post(DISCARD, "not json", KEY, "wrong"), 403, "unknown key");

            assertThat(probe.reads).as("nothing read the body").hasValue(0);
            assertThat(server.bean(Spy.class).ran).isEmpty();
            assertThat(server.post(DISCARD, "not json", KEY, "secret").statusCode()).as("the same body, allowed")
                .isEqualTo(400);
            assertThat(probe.reads.get()).as("so the probe can see it").isPositive();
            assertThat(server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret").body()).as("the letter was still there")
                .isEqualTo("{\"count\":1}");
        }
    }

    /** A POST another site sent, perhaps with the operator's cookie, is refused before the authorizer. */
    @ParameterizedTest
    @ValueSource(strings = {"cross-site", "same-site"})
    void aPostAnotherSiteSentIsRefusedBeforeTheAuthorizer(String site) throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class); var stderr = new CapturedStderr()) {
            var refused = server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret", "Sec-Fetch-Site", site);

            assertError(refused, 403, "cross-site request refused");
            assertThat(server.bean(Keys.class).threads).as("the authorizer never ran").isEmpty();
            assertThat(server.bean(Spy.class).ran).isEmpty();
            assertThat(audit(stderr)).containsExactly("outbox admin POST \"" + DISCARD + "\" denied: 403");
        }
    }

    /** Clients other than browsers send no such header; a GET changes nothing, so a link from another site works. */
    @ParameterizedTest
    @CsvSource({"POST,", "POST,same-origin", "POST,none", "GET,cross-site", "GET,same-site", "HEAD,cross-site"})
    void aCallOfTheSameSiteOrNoBrowserOrAReadReachesTheAuthorizer(String method, String site) throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class)) {
            var path = method.equals("POST") ? API + "/recovery/xid-regression" : STATUS;
            var headers = site == null ? new String[] {KEY, "wrong"}
                : new String[] {KEY, "wrong", "Sec-Fetch-Site", site};

            assertThat(server.send(method, path, null, headers).statusCode()).isEqualTo(403);

            assertThat(server.bean(Keys.class).threads).hasSize(1);
        }
    }

    static Stream<OutboxAdminAuthorizer> failingAuthorizers() {
        return Stream.of(
            request -> {
                throw new IllegalStateException("key store down");
            },
            request -> {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "key store down");
            },
            request -> {
                throw new AssertionError("key store down");
            },
            request -> null);
    }

    /** A thrown 401 is no refusal, as the authorizer's javadoc promises; the caller learns nothing of why. */
    @ParameterizedTest
    @MethodSource("failingAuthorizers")
    void anAuthorizerThatFailsIsAFixed500(OutboxAdminAuthorizer failing) throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class); var stderr = new CapturedStderr()) {
            server.bean(Keys.class).answer = failing;

            assertError(server.get(STATUS), 500, "authorization failed");

            var call = "outbox admin GET \"" + STATUS + "\"";
            assertThat(server.bean(Spy.class).ran).isEmpty();
            assertThat(audit(stderr)).containsExactly(call + " denied: 500");
            assertThat(warnings(stderr)).as("one, and none of the controller's")
                .singleElement().asString().contains(call + ": the OutboxAdminAuthorizer failed, answered 500");
            assertThat(stderr.text()).as("the WARN has the exception").containsPattern("key store down|returned null");
        }
    }

    @Test
    void theAuthorizerRunsOnTheRequestsThread() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class)) {
            assertThat(server.get(STATUS, KEY, "secret").statusCode()).isEqualTo(200);

            assertThat(server.bean(Keys.class).threads).isEqualTo(server.bean(Spy.class).threads);
        }
    }

    @Test
    void anAuditLineIsOnePerCallWithTheIdentityAndTheOperationsDetail() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            var abandon = API + "/waiting/abandon";

            assertThat(server.post(abandon, "{\"topic\":\"t\",\"reason\":\"r\"}", KEY, "secret").statusCode())
                .isEqualTo(200);
            awaitAudit(stderr, 1);
            server.get(STATUS, KEY, "secret");
            awaitAudit(stderr, 2);
            server.get(STATUS + "?x=1", KEY, "wrong");
            server.get(STATUS);

            assertThat(audit(stderr)).containsExactly(
                "outbox admin POST \"" + abandon + "\" by \"tester\" with topic \"t\", key null, reason \"r\","
                    + " dryRun \"false\": 200",
                "outbox admin GET \"" + STATUS + "\" by \"tester\": 200",
                "outbox admin GET \"" + STATUS + "\" denied: 403",
                "outbox admin GET \"" + STATUS + "\" denied: 401");
        }
    }

    /** An identity may be caller text, a token's subject, and the reason is: neither can forge a line or a status. */
    @Test
    void anAuditValueWithAQuoteAndANewlineStaysOneEscapedLine() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.bean(Keys.class).answer = request -> OutboxAdminAuthorizer.allow("x\": 200\nINFO forged \\");

            var response = server.post(API + "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"v2\\\", dryRun \\\"true\\r\\nINFO forged\\u2028\\u0007\"}");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(awaitAudit(stderr, 1)).containsExactly("outbox admin POST \"" + API + "/waiting/abandon\" by"
                + " \"x\\\": 200\\nINFO forged \\\\\" with topic \"t\", key null, reason"
                + " \"v2\\\", dryRun \\\"true\\r\\nINFO forged\\u2028\\u0007\", dryRun \"false\": 200");
            assertThat(stderr.text()).doesNotContain("\nINFO forged");
        }
    }

    /** {@code {table}} puts caller text into the path before any authorization; the query stays out of the line. */
    @Test
    void anAuditLineHasThePathAsSentWithoutTheQuery() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            var path = API + "/orphans/x%22%20by%20root:%20200%0D%0AINFO%20forged/restore";

            assertThat(server.post(path + "?fromXid=0&secret=s", null).statusCode()).isEqualTo(401);

            assertThat(audit(stderr)).containsExactly("outbox admin POST \"" + path + "\" denied: 401");
        }
    }

    /** A container refuses these raw, so no HTTP call gets them here. */
    @Test
    void anAuditLinePathIsOneQuotedToken() throws Exception {
        var guard = new OutboxAdminGuard(request -> OutboxAdminAuthorizer.unauthorized("who?"));
        var handler = new HandlerMethod(new OutboxAdminController(null),
            OutboxAdminController.class.getMethod("getStatus"));
        try (var stderr = new CapturedStderr()) {
            assertThatThrownBy(() -> guard.preHandle(new MockHttpServletRequest("GET", "/a\\\" by root: 200 \"\n"),
                new MockHttpServletResponse(), handler)).isInstanceOf(OutboxAdminGuard.Refused.class);

            assertThat(audit(stderr))
                .containsExactly("outbox admin GET \"/a\\\\\\\" by root: 200 \\\"\\n\" denied: 401");
        }
    }

    /** With no resolver to answer it, a failure goes to the container: the status MVC reports is not yet the 500. */
    @Configuration(proxyBeanMethods = false)
    static class NoExceptionHandling implements WebMvcConfigurer {

        @Override
        public void configureHandlerExceptionResolvers(List<HandlerExceptionResolver> resolvers) {
            resolvers.add((request, response, handler, ex) -> null);
        }
    }

    /** Refused all the same; the allowed failure has its 500 and one line, none for the container's error dispatch. */
    @Test
    void withoutExceptionHandlingADenialStaysClosedAndAFailureIsAudited() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithSpy.class, NoExceptionHandling.class);
            var stderr = new CapturedStderr()) {
            server.execute("drop table " + server.names.dead() + " cascade");

            assertThat(server.get(API + "/dead-letters/count").statusCode()).isEqualTo(500);
            assertThat(server.bean(Spy.class).ran).isEmpty();
            assertThat(server.get(API + "/dead-letters/count", KEY, "secret").statusCode()).isEqualTo(500);

            var call = "outbox admin GET \"" + API + "/dead-letters/count\"";
            awaitAudit(stderr, 2);
            server.get(STATUS);
            assertThat(awaitAudit(stderr, 3)).containsExactly(call + " denied: 401",
                call + " by \"tester\": 500", "outbox admin GET \"" + STATUS + "\" denied: 401");
        }
    }

    @Test
    void aRouteFailureIsWarnedWithTheIdentityOnceAndAuditedAs500() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.bean(Keys.class).answer = request -> OutboxAdminAuthorizer.allow("ops\n");
            server.execute("drop table " + server.names.dead() + " cascade");

            assertThat(server.get(API + "/dead-letters/count").statusCode()).isEqualTo(500);

            var call = "outbox admin GET \"" + API + "/dead-letters/count\" by \"ops\\n\"";
            assertThat(awaitAudit(stderr, 1)).containsExactly(call + ": 500");
            assertThat(warnings(stderr)).singleElement().asString().contains(call + " failed, answered 500");
        }
    }

    @Test
    void aDenialIsNotWarned() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.get(STATUS);
            server.post(DISCARD, "{}", "Sec-Fetch-Site", "cross-site");

            assertThat(audit(stderr)).hasSize(2);
            assertThat(warnings(stderr)).isEmpty();
        }
    }

    private static List<String> warnings(CapturedStderr stderr) {
        return stderr.text().lines().filter(l -> l.contains(" WARN ") && l.contains("outbox admin")).toList();
    }

    // ------------------------------------------------------------------------------------------------ at the start

    private static WebApplicationContextRunner runner(Class<?>... configurations) {
        return new WebApplicationContextRunner().withUserConfiguration(AdminServer.Base.class)
            .withUserConfiguration(configurations).withPropertyValues(AdminHttpIT.properties());
    }

    @Test
    void noAuthorizerFailsTheStartNamingIt() {
        runner().run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining(
                "no OutboxAdminAuthorizer bean: the admin API has no other protection, so it does not start without"
                    + " one; define a bean");
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoAuthorizers {

        @Bean
        OutboxAdminAuthorizer first() {
            return request -> OutboxAdminAuthorizer.forbidden("first");
        }

        @Bean
        OutboxAdminAuthorizer second() {
            return request -> OutboxAdminAuthorizer.forbidden("second");
        }
    }

    @Test
    void severalAuthorizersWithoutAPrimaryFailTheStartNamingThem() {
        runner(TwoAuthorizers.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("several OutboxAdminAuthorizer beans:")
                .hasStackTraceContaining("first").hasStackTraceContaining("second");
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class PrimaryAuthorizer {

        @Bean
        @Primary
        OutboxAdminAuthorizer primary() {
            return request -> OutboxAdminAuthorizer.forbidden("primary");
        }
    }

    @Test
    void aPrimaryAuthorizerIsTheOneTaken() throws Exception {
        try (var server = AdminServer.start(TwoAuthorizers.class, PrimaryAuthorizer.class)) {
            assertError(server.get(STATUS), 403, "primary");
        }
    }

    @Test
    void outboxDisabledStartsWithoutAnAuthorizerAndExposesNothing() {
        new WebApplicationContextRunner().withUserConfiguration(AdminServer.Base.class)
            .withPropertyValues(AdminHttpIT.properties("outbox.enabled=false")).run(context -> {
                assertThat(context).hasNotFailed().doesNotHaveBean(OutboxAdminController.class)
                    .doesNotHaveBean("outboxAdminGuard");
            });
    }

    // ------------------------------------------------------------------------------------------------- the decision

    @Test
    void denyIsA401OrA403WithAMessage() {
        assertThat(OutboxAdminAuthorizer.unauthorized("who?")).isEqualTo(new Deny(401, "who?"));
        assertThat(OutboxAdminAuthorizer.forbidden("no")).isEqualTo(new Deny(403, "no"));
        for (int status : new int[] {200, 400, 404, 500}) {
            assertThatThrownBy(() -> new Deny(status, "x")).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> OutboxAdminAuthorizer.forbidden(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void allowNeedsAnIdentity() {
        assertThat(OutboxAdminAuthorizer.allow("ops")).isEqualTo(new Allow("ops"));
        assertThatThrownBy(() -> OutboxAdminAuthorizer.allow(null)).isInstanceOf(NullPointerException.class);
    }
}
