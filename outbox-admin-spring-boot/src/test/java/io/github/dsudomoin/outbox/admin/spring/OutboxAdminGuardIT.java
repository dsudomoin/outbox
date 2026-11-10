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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.handler.HandlerMappingIntrospector;
import org.springframework.web.servlet.handler.MappedInterceptor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.servlet.mvc.method.annotation.RequestResponseBodyMethodProcessor;

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

        private static final Set<String> NO_ROUTES = Set.of("denied", "guardRan");

        final List<String> mapped = new CopyOnWriteArrayList<>();
        final List<String> ran = new CopyOnWriteArrayList<>();
        final List<String> threads = new CopyOnWriteArrayList<>();

        @Around("within(io.github.dsudomoin.outbox.admin.spring.OutboxAdminController)")
        Object around(ProceedingJoinPoint call) throws Throwable {
            if (!NO_ROUTES.contains(call.getSignature().getName())) {
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
                    "discardDeadLetters", "restoreOrphan", "recoverStrandedShards", "recoverXidRegression", "abandonKey",
                    "abandonWaiting", "notJson");
            assertThat(routes.stream().filter(r -> r.path().endsWith("/orphans/x/restore"))).isNotEmpty();
            assertThat(routes).as("ten of the contract and four of notJson").hasSizeGreaterThanOrEqualTo(14);
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
            assertThat(server.settledAudit(stderr, 1))
                .containsExactly("outbox admin POST \"" + DISCARD + "\" denied: 403");
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
            assertThat(server.settledAudit(stderr, 1)).containsExactly(call + " denied: 500");
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
            server.send("HEAD", STATUS, null, KEY, "secret");
            awaitAudit(stderr, 3);
            assertThat(server.send("POST", DISCARD, "x", KEY, "secret").statusCode()).as("notJson").isEqualTo(400);
            awaitAudit(stderr, 4);
            server.send("HEAD", STATUS, null);
            server.get(STATUS + "?x=1", KEY, "wrong");
            server.get(STATUS);

            assertThat(server.settledAudit(stderr, 7)).containsExactly(
                "outbox admin POST \"" + abandon + "\" by \"tester\" with topic \"t\", key null, reason \"r\","
                    + " dryRun \"false\": 200",
                "outbox admin GET \"" + STATUS + "\" by \"tester\": 200",
                "outbox admin HEAD \"" + STATUS + "\" by \"tester\": 200",
                "outbox admin POST \"" + DISCARD + "\" by \"tester\": 400",
                "outbox admin HEAD \"" + STATUS + "\" denied: 401",
                "outbox admin GET \"" + STATUS + "\" denied: 403",
                "outbox admin GET \"" + STATUS + "\" denied: 401");
        }
    }

    /** An identity may be caller text, a token's subject, and the reason is: neither can forge a line or a status. */
    @Test
    void anAuditValueWithAQuoteAndANewlineStaysOneEscapedLine() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.bean(Keys.class).answer = request -> OutboxAdminAuthorizer.allow("x\": 200\nINFO forged \\\u2029");

            var response = server.post(API + "/waiting/abandon",
                "{\"topic\":\"t\",\"reason\":\"v2\\\", dryRun \\\"true\\r\\nINFO forged\\u2028\\u2029\\u0007\"}");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(server.settledAudit(stderr, 1)).containsExactly("outbox admin POST \"" + API
                + "/waiting/abandon\" by \"x\\\": 200\\nINFO forged \\\\\\u2029\" with topic \"t\", key null, reason"
                + " \"v2\\\", dryRun \\\"true\\r\\nINFO forged\\u2028\\u2029\\u0007\", dryRun \"false\": 200");
            assertThat(stderr.text()).doesNotContain("\nINFO forged").doesNotContain("\u2028").doesNotContain("\u2029");
        }
    }

    /** {@code {table}} puts caller text into the path before any authorization, decoded by the container. */
    @Test
    void anAuditLineEscapesThePath() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            assertThat(server.post(API + "/orphans/x%0D%0AINFO%20forged%09%07/restore?fromXid=0", null).statusCode())
                .isEqualTo(401);

            assertThat(server.settledAudit(stderr, 1)).singleElement().asString()
                .endsWith("orphans/x\\r\\nINFO forged\\t\\u0007/restore\" denied: 401");
            assertThat(stderr.text()).doesNotContain("\nINFO forged");
        }
    }

    /** Decoded, a path carries what a caller sends: a bidi override must not reorder the line on an operator's screen. */
    @Test
    void anAuditLineEscapesFormatCharacters() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.post(API + "/orphans/x%E2%80%AEy%E2%80%8Bz%F3%A0%80%81/restore?fromXid=0", null); // U+E0001 last

            assertThat(server.settledAudit(stderr, 1)).singleElement().asString()
                .endsWith("orphans/x\\u202ey\\u200bz\\udb40\\udc01/restore\" denied: 401");
            assertThat(stderr.text()).doesNotContain("\u202e").doesNotContain("\u200b")
                .doesNotContain(new String(Character.toChars(0xE0001)));
        }
    }

    /** Quoted, its quotes and backslashes escaped, a path cannot end early and pass for the rest of a line. */
    @Test
    void anAuditLineQuotesThePath() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.post(API + "/orphans/x%20by%20root:%20200/restore?fromXid=0", null);
            server.post(API + "/orphans/x%22%20by%20root:%20200%20%22/restore?fromXid=0", null);

            assertThat(server.settledAudit(stderr, 2)).containsExactly(
                "outbox admin POST \"" + API + "/orphans/x by root: 200/restore\" denied: 401",
                "outbox admin POST \"" + API + "/orphans/x\\\" by root: 200 \\\"/restore\" denied: 401");
        }
    }

    /** A query or a {@code ;} part may carry a secret, and a context path is no part of the route. */
    @Test
    void anAuditLineHasTheRoutesPathAlone() throws Exception {
        try (var server = AdminServer.start(List.of("server.servlet.context-path=/ctx"), WithKeys.class);
            var stderr = new CapturedStderr()) {
            server.get("/ctx" + STATUS + "?token=s");
            server.get("/ctx" + STATUS + ";jsessionid=s;token=s");
            server.get("/ctx" + STATUS); // so a late duplicate of those two shows too

            assertThat(audit(stderr)).hasSize(3).containsOnly("outbox admin GET \"" + STATUS + "\" denied: 401");
        }
    }

    /** A container refuses these raw, so no HTTP call gets them here. */
    @Test
    void anAuditLinePathIsOneQuotedToken() throws Exception {
        var guard = new OutboxAdminGuard(() -> request -> OutboxAdminAuthorizer.unauthorized("who?"));
        var handler = new HandlerMethod(new OutboxAdminController(null),
            OutboxAdminController.class.getMethod("getStatus"));
        try (var stderr = new CapturedStderr()) {
            var request = new MockHttpServletRequest("GET", "/x");
            request.setPathInfo("/a\\\" by root: 200 \"\n");
            assertThatThrownBy(() -> guard.preHandle(request, new MockHttpServletResponse(), handler))
                .isInstanceOf(OutboxAdminGuard.Refused.class);

            assertThat(audit(stderr))
                .containsExactly("outbox admin GET \"/a\\\\\\\" by root: 200 \\\"\\n\" denied: 401");
        }
    }

    /** The guard takes the authorizer at the first call; it is a failure of the authorizer's like any other. */
    @Test
    void anAuthorizerThatCannotBeHadIsAFixed500() throws Exception {
        OutboxAdminGuard guard = new OutboxAdminGuard(() -> {
            throw new IllegalStateException("no bean");
        });
        var handler = new HandlerMethod(new OutboxAdminController(null),
            OutboxAdminController.class.getMethod("getStatus"));

        assertThatThrownBy(() -> guard.preHandle(new MockHttpServletRequest("GET", STATUS),
            new MockHttpServletResponse(), handler)).isInstanceOfSatisfying(OutboxAdminGuard.Refused.class, refused -> {
                assertThat(refused.status).isEqualTo(500);
                assertThat(refused).hasMessage("authorization failed");
            });
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
            assertThat(server.settledAudit(stderr, 2)).containsExactly(call + " denied: 401",
                call + " by \"tester\": 500");
        }
    }

    @Test
    void aRouteFailureIsWarnedWithTheIdentityOnceAndAuditedAs500() throws Exception {
        try (var server = AdminServer.start(WithKeys.class); var stderr = new CapturedStderr()) {
            server.bean(Keys.class).answer = request -> OutboxAdminAuthorizer.allow("ops\n");
            server.execute("drop table " + server.names.dead() + " cascade");

            assertThat(server.get(API + "/dead-letters/count").statusCode()).isEqualTo(500);

            var call = "outbox admin GET \"" + API + "/dead-letters/count\" by \"ops\\n\"";
            assertThat(server.settledAudit(stderr, 1)).containsExactly(call + ": 500");
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

    // ------------------------------------------------------------------------- when the guard could have been lost

    /** A route catalog, say: what an authorizer may need that needs the mapping, while the mapping is being made. */
    static class RouteCatalog {

        RouteCatalog(RequestMappingHandlerMapping mapping) {
            assertThat(mapping).isNotNull();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AuthorizerNeedsTheMapping {

        @Bean
        RouteCatalog routeCatalog(@Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping mapping) {
            return new RouteCatalog(mapping);
        }

        @Bean
        Keys keys(RouteCatalog catalog) {
            return new Keys();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AuthorizerNeedsTheIntrospector {

        @Bean
        Keys keys(HandlerMappingIntrospector introspector) {
            return new Keys();
        }
    }

    static Stream<Arguments> authorizersThatReachTheMapping() {
        return Stream.of(AuthorizerNeedsTheMapping.class, AuthorizerNeedsTheIntrospector.class)
            .flatMap(app -> Stream.of(Arguments.of(app, List.of()),
                Arguments.of(app, List.of("spring.main.lazy-initialization=true"))));
    }

    /** The mapping's start asks for the guard, which must not ask for the authorizer, which waits for the mapping. */
    @ParameterizedTest
    @MethodSource("authorizersThatReachTheMapping")
    void anAuthorizerThatReachesTheMappingStillGuardsEveryCall(Class<?> app, List<String> properties) throws Exception {
        try (var server = AdminServer.start(properties, app)) {
            letter(server);

            assertError(server.post(DISCARD, "{\"ids\":[1]}"), 401, "X-Api-Key required");
            assertError(server.post(DISCARD, "{\"ids\":[1]}", KEY, "wrong"), 403, "unknown key");

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
            assertThat(server.bean(Keys.class).threads).as("the authorizer was asked").hasSize(2);
            assertThat(server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret").body()).isEqualTo("{\"count\":1}");
        }
    }

    @Test
    void noAuthorizerFailsTheStartUnderLazyInitialization() {
        assertThatThrownBy(() -> AdminServer.start(List.of("spring.main.lazy-initialization=true")))
            .isExactlyInstanceOf(IllegalStateException.class).hasNoCause()
            .hasMessageStartingWith("no OutboxAdminAuthorizer bean");
    }

    /** An app's mapping ahead of MVC's, which takes the handler and leaves the interceptors behind. */
    @Configuration(proxyBeanMethods = false)
    static class DropsTheInterceptors {

        @Bean
        HandlerMapping handlerOnly(
            @Qualifier("requestMappingHandlerMapping") ObjectProvider<RequestMappingHandlerMapping> mapping) {
            return new OrderedHandlerOnly(mapping);
        }
    }

    record OrderedHandlerOnly(ObjectProvider<RequestMappingHandlerMapping> mapping) implements HandlerMapping, Ordered {

        @Override
        public HandlerExecutionChain getHandler(HttpServletRequest request) throws Exception {
            var chain = mapping.getObject().getHandler(request);
            return chain == null ? null : new HandlerExecutionChain(chain.getHandler());
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE;
        }
    }

    /** Whatever lost the guard, the controller itself refuses, for every route, before any body is read. */
    @Test
    void aMappingThatDropsTheGuardOpensNoRoute() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, WithBodyProbe.class, DropsTheInterceptors.class);
            var stderr = new CapturedStderr()) {
            letter(server);

            for (var route : server.routes()) {
                var refused = route.contentType() == null ? server.send(route.method(), route.path(), null)
                    : server.send(route.method(), route.path(), "{\"ids\":[1]}", "Content-Type", route.contentType());

                assertError(refused, 500, "authorization did not run");
            }

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
            assertThat(server.bean(BodyProbe.class).reads).as("nothing read a body").hasValue(0);
            assertThat(server.bean(Keys.class).threads).isEmpty();
            int routes = server.routes().size();
            assertThat(server.settledAudit(stderr, routes)).hasSize(routes).allMatch(l -> l.endsWith(" denied: 500"));
            assertThat(stderr.text().lines().filter(l -> l.contains(" WARN ") && l.contains("not on the handler")))
                .as("one WARN per call, the sentinel too, naming the cause").hasSize(routes + 1);
        }
    }

    /** An app's advice whose model entry has the name of {@code guardRan}, so MVC skips that method. */
    @ControllerAdvice
    static class SkipsGuardRan {

        @ModelAttribute
        public void add(Model model) {
            model.addAttribute("", "x");
        }
    }

    /** Past {@code guardRan}, each route still has to ask for the admin. */
    @Test
    void aSkippedGuardRanStillReachesNoAdmin() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, DropsTheInterceptors.class, SkipsGuardRan.class)) {
            letter(server);

            assertError(server.post(DISCARD, "{\"ids\":[1]}"), 500, "authorization did not run");

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
        }
    }

    /** A body resolver of the app's own, ahead of MVC's, without its advice chain: the reader never sees the body. */
    @Configuration(proxyBeanMethods = false)
    static class SkipsTheReader {

        @Bean
        static BeanPostProcessor bodyResolverAhead() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String name) {
                    if (bean instanceof RequestMappingHandlerAdapter adapter) {
                        var resolvers = new ArrayList<HandlerMethodArgumentResolver>();
                        resolvers.add(new RequestResponseBodyMethodProcessor(adapter.getMessageConverters()));
                        resolvers.addAll(adapter.getArgumentResolvers());
                        adapter.setArgumentResolvers(resolvers);
                    }
                    return bean;
                }
            };
        }
    }

    @Test
    void aBodyTheReaderDidNotSeeActsOnNothing() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, SkipsTheReader.class)) {
            letter(server);

            assertError(server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret"), 500, "the request body was not checked");

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
        }
    }

    /** An app's filter that copies parameters into the attributes hands a caller any attribute name. */
    @Configuration(proxyBeanMethods = false)
    static class ForgesTheIdentity {

        @Bean
        OncePerRequestFilter copiesParameters() {
            return new OncePerRequestFilter() {
                @Override
                protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                        FilterChain chain) throws ServletException, IOException {
                    request.setAttribute(OutboxAdminGuard.class.getName() + ".identity", "forged");
                    chain.doFilter(request, response);
                }
            };
        }
    }

    @Test
    void anAttributeACallerCanSetPassesForNoIdentity() throws Exception {
        try (var server = AdminServer.start(WithKeys.class, DropsTheInterceptors.class, ForgesTheIdentity.class)) {
            letter(server);

            assertError(server.post(DISCARD, "{\"ids\":[1]}"), 500, "authorization did not run");

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
        }
        try (var server = AdminServer.start(WithKeys.class, ThrowsAheadOfTheGuard.class, ForgesTheIdentity.class)) {
            assertError(server.post(DISCARD, "{}", "X-Kind", "iae"), 500, "authorization failed");
        }
    }

    /** {@code @RestController} makes the controller a scan's find: a route with no auto-configuration to guard it. */
    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackages = "io.github.dsudomoin.outbox.admin.spring", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.REGEX,
            pattern = ".*\\.(AdminHttpIT|AdminCoexistenceIT|OutboxAdminGuardIT|AdminServer)($|\\$.*)"),
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class)})
    static class ScansTheModule {}

    @Test
    void aControllerScannedWithoutTheAutoConfigurationOpensNoRoute() throws Exception {
        try (var server = AdminServer.start(List.of("spring.autoconfigure.exclude="
            + OutboxAdminAutoConfiguration.class.getName()), ScansTheModule.class, WithKeys.class)) {
            assertThat(server.context.getBeanNamesForType(OutboxAdminController.class)).hasSize(1);
            assertThat(server.context.containsBean("outboxAdminGuard")).isFalse();
            letter(server);

            assertError(server.post(DISCARD, "{\"ids\":[1]}", KEY, "secret"), 500, "authorization did not run");
            assertError(server.get(STATUS), 500, "authorization did not run");

            assertThat(letters(server)).as("the letter stays").isEqualTo(1);
        }
    }

    /** An app's interceptor, a tenant lookup say: a bean of the user's configuration, so ahead of the guard. */
    @Configuration(proxyBeanMethods = false)
    static class ThrowsAheadOfTheGuard {

        @Bean
        MappedInterceptor throwsAhead() {
            return new MappedInterceptor(null, new HandlerInterceptor() {
                @Override
                public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
                    throw switch (request.getHeader("X-Kind")) {
                        case "iae" -> new IllegalArgumentException("secret text");
                        case "ise" -> new IllegalStateException("secret text");
                        case "status" -> new ResponseStatusException(HttpStatus.CONFLICT, "secret text");
                        default -> new RuntimeException("secret text");
                    };
                }
            });
        }
    }

    /** The handler is already this controller, so its handlers answer: not with text for someone nobody authorized. */
    @ParameterizedTest
    @ValueSource(strings = {"runtime", "iae", "ise", "status"})
    void aFailureAheadOfTheGuardShowsNoTextToTheCaller(String kind) throws Exception {
        try (var server = AdminServer.start(WithKeys.class, ThrowsAheadOfTheGuard.class);
            var stderr = new CapturedStderr()) {
            var failed = server.post(DISCARD, "{}", "X-Kind", kind);

            assertError(failed, 500, "authorization failed");
            assertThat(server.bean(Keys.class).threads).isEmpty();
            assertThat(warnings(stderr)).singleElement().asString().contains("failed, answered 500");
            assertThat(stderr.text()).as("the WARN has the exception").contains("secret text");
            assertThat(server.settledAudit(stderr, 1)).containsExactly("outbox admin POST \"" + DISCARD + "\" denied: 500");
        }
    }

    private static void letter(AdminServer server) throws SQLException {
        server.execute("insert into " + server.names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
            + " values (1, 0, gen_random_uuid(), 't', now(), 3)");
    }

    private static long letters(AdminServer server) throws SQLException {
        try (var c = server.bean(DataSource.class).getConnection()) {
            return TestSql.queryLong(c, "select count(*) from " + server.names.dead());
        }
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
            assertThat(context.getStartupFailure()).isExactlyInstanceOf(IllegalStateException.class).hasNoCause()
                .hasMessageStartingWith("no OutboxAdminAuthorizer bean: the admin API has no other protection, so it"
                    + " does not start without one; define a bean");
        });
    }

    /** What the operator reads: Boot's failure analyzer would put its own text in place of this one, given a cause. */
    @Test
    void aRealStartWithoutAnAuthorizerReportsTheAdvice() {
        try (var stderr = new CapturedStderr()) {
            assertThatThrownBy(AdminServer::start).isExactlyInstanceOf(IllegalStateException.class).hasNoCause()
                .hasMessageContaining("no OutboxAdminAuthorizer bean");

            assertThat(stderr.text()).contains("no OutboxAdminAuthorizer bean")
                .doesNotContain("Consider defining a bean");
        }
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
            assertThat(context.getStartupFailure()).isExactlyInstanceOf(IllegalStateException.class).hasNoCause()
                .hasMessageStartingWith("several OutboxAdminAuthorizer beans: ").hasMessageContaining("first")
                .hasMessageContaining("second");
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
