package io.github.dsudomoin.outbox.admin.kora;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.admin.kora.model.$ModelError_JsonWriter;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.http.common.body.HttpBodyInput;
import ru.tinkoff.kora.http.common.cookie.Cookie;
import ru.tinkoff.kora.http.common.header.HttpHeaders;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.HttpServerResponseException;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;

class OutboxAdminInterceptorTest {

    /**
     * The authorizer's stage completes only after the caller has attached to the response stage, as on a real
     * executor's thread: then {@code CompletableFuture} runs dependents last-attached first, and an audit line on a
     * side branch of that stage would come after the response.
     */
    @Test
    void authorizerFailureIsAuditedBeforeTheResponseCompletes() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw new IllegalStateException("key store down");
        }, deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var logAtResponse = new AtomicReference<String>();
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    }).whenComplete((r, e) -> logAtResponse.set(stderr.text()));

            authorization.get().run();

            assertThat(logAtResponse.get()).contains("outbox admin GET \"/kora-outbox/admin/v1/status\" denied: 500");
            assertThat(response.toCompletableFuture().join().code()).isEqualTo(500);
        }
    }

    /** As the authorizer's javadoc promises: a thrown 401 is no refusal, it is a failure. */
    @Test
    void anAuthorizerThrowing401GetsTheFixed500() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw HttpServerResponseException.of(401, "token expired");
        }, deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    });
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(500);
            assertThat(body(answered)).isEqualTo("{\"message\":\"authorization failed\"}");
            assertThat(stderr.text()).contains("WARN").contains("OutboxAdminAuthorizer failed")
                    .as("the exception, which prints its code, not its text")
                    .contains("HttpServerResponseException{code=401")
                    .contains("outbox admin GET \"/kora-outbox/admin/v1/status\" denied: 500");
        }
    }

    /** A failure without a message is named by its class; the WARN has the route and the identity, escaped. */
    @Test
    void aRouteFailureIsA500Error() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops\n"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.failedFuture(new IllegalMonitorStateException()));
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(500);
            assertThat(body(answered)).isEqualTo("{\"message\":\"java.lang.IllegalMonitorStateException\"}");
            var call = "outbox admin GET \"/kora-outbox/admin/v1/status\" by \"ops\\n\"";
            assertThat(stderr.text()).contains("WARN " + OutboxAdminInterceptor.class.getName() + " - " + call
                    + " failed, answered 500").contains("INFO " + OutboxAdminInterceptor.class.getName() + " - "
                    + call + ": 500");
        }
    }

    /** The server's own response exceptions keep their status, 5xx included; only other failures become 500. */
    @Test
    void aServerResponseExceptionKeepsItsStatus() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.failedFuture(HttpServerResponseException.of(503,
                            "draining")));
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(503);
            assertThat(body(answered)).isEqualTo("{\"message\":\"draining\"}");
            var call = "outbox admin GET \"/kora-outbox/admin/v1/status\" by \"ops\"";
            assertThat(stderr.text()).contains("WARN " + OutboxAdminInterceptor.class.getName() + " - " + call
                    + " failed, answered 503").contains(call + ": 503");
        }
    }

    /** Undertow leaves {@code %5C} encoded, so a backslash in a path is tested here. */
    @Test
    void auditLinePathIsOneQuotedToken() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.unauthorized("who?"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/a\\\" by root: 200 \""),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    });
            authorization.get().run();

            assertThat(response.toCompletableFuture().join().code()).isEqualTo(401);
            assertThat(stderr.text()).contains("outbox admin GET \"/a\\\\\\\" by root: 200 \\\"\" denied: 401");
        }
    }

    /** The identity may be caller text (a token's subject): quoted, it cannot pass for the status that follows. */
    @Test
    void auditLineQuotesTheIdentity() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("x\": 200 \\"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(409)));
            authorization.get().run();

            assertThat(response.toCompletableFuture().join().code()).isEqualTo(409);
            assertThat(stderr.text().lines().filter(l -> l.contains("outbox admin ")))
                    .singleElement(InstanceOfAssertFactories.STRING)
                    .endsWith("outbox admin GET \"/kora-outbox/admin/v1/status\" by \"x\\\": 200 \\\\\": 409");
        }
    }

    /**
     * A browser names the site that made it send a request; a POST another site sent, perhaps with the operator's
     * cookie, SSO session or network address, is refused before the authorizer, which could not tell it apart.
     */
    @ParameterizedTest
    @ValueSource(strings = {"cross-site", "same-site"})
    void aPostAnotherSiteSentIsRefusedBeforeTheAuthorizer(String site) {
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw new AssertionError("the authorizer must not run");
        }, deferred(new AtomicReference<>()), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("POST",
                    "/kora-outbox/admin/v1/dead-letters/discard", HttpHeaders.of("sec-fetch-site", site)),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    }).toCompletableFuture();

            assertThat(response).as("answered without the authorizer").isDone();
            var answered = response.join();
            assertThat(answered.code()).isEqualTo(403);
            assertThat(body(answered)).isEqualTo("{\"message\":\"cross-site request refused\"}");
            assertThat(stderr.text())
                    .contains("outbox admin POST \"/kora-outbox/admin/v1/dead-letters/discard\" denied: 403");
        }
    }

    /**
     * Over plain HTTP to an address other than localhost a browser sends no {@code Sec-Fetch-Site}, but it names the
     * page's origin: one that is not this server's own host and port is another site.
     */
    @ParameterizedTest
    @CsvSource({"http://evil.example,app:8080", "null,app:8080", "http://app:8081,app:8080", "http://app,app:8080",
            "http://app:8080,app", "http://app:8080,"})
    void aPostWhoseOriginIsAnotherHostIsRefusedBeforeTheAuthorizer(String origin, String host) {
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw new AssertionError("the authorizer must not run");
        }, deferred(new AtomicReference<>()), new $ModelError_JsonWriter());

        var response = interceptor.intercept(Context.current(), request("POST",
                "/kora-outbox/admin/v1/recovery/xid-regression", host == null ? HttpHeaders.of("origin", origin)
                        : HttpHeaders.of("origin", origin, "host", host)),
                (context, request) -> {
                    throw new AssertionError("the route must not run");
                }).toCompletableFuture();

        assertThat(response).as("answered without the authorizer").isDone();
        var answered = response.join();
        assertThat(answered.code()).isEqualTo(403);
        assertThat(body(answered)).isEqualTo("{\"message\":\"cross-site request refused\"}");
    }

    /** The scheme is not compared: behind a proxy ending TLS the server sees http where the page had https. */
    @ParameterizedTest
    @CsvSource({"http://app:8080,app:8080", "http://APP:8080,app:8080", "http://app,app", "http://app:80,app",
            "https://app,app:443", "https://app:8443,app:8443", "http://[::1]:8080,[::1]:8080",
            "http://admin.internal,admin.internal"})
    void aPostFromTheServersOwnOriginReachesTheAuthorizer(String origin, String host) {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        var response = interceptor.intercept(Context.current(), request("POST",
                "/kora-outbox/admin/v1/recovery/xid-regression", HttpHeaders.of("origin", origin, "host", host)),
                (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(200)));
        authorization.get().run();

        assertThat(response.toCompletableFuture().join().code()).isEqualTo(200);
    }

    /** {@code Sec-Fetch-Site} decides when a browser sends it; a GET with another origin changes nothing. */
    @ParameterizedTest
    @CsvSource({"POST,same-origin", "GET,"})
    void anOriginIsNotLookedAtWithSecFetchSiteOrOnAGet(String method, String site) {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        var headers = HttpHeaders.of("origin", "http://proxy.example", "host", "app:8080");
        if (site != null) {
            headers.set("sec-fetch-site", site);
        }
        var response = interceptor.intercept(Context.current(), request(method, "/kora-outbox/admin/v1/status",
                headers), (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(200)));
        authorization.get().run();

        assertThat(response.toCompletableFuture().join().code()).isEqualTo(200);
    }

    /**
     * Kora reads a JSON body whatever its {@code Content-Type}, and a page elsewhere can post {@code text/plain}
     * without asking the server first: a body route takes {@code application/json} only, as Spring MVC's does.
     */
    @ParameterizedTest
    @CsvSource({"text/plain,415", "application/x-www-form-urlencoded,415", "multipart/form-data; boundary=x,415",
            "application/jsonx,415", ",400"})
    void aBodyRouteRefusesAnotherContentTypeBeforeTheRoute(String type, int status) {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            for (var path : List.of("dead-letters/discard", "dead-letters/redrive", "keys/abandon",
                    "waiting/abandon")) {
                var response = interceptor.intercept(Context.current(), request("POST",
                        "/kora-outbox/admin/v1/" + path, type == null ? HttpHeaders.of()
                                : HttpHeaders.of("content-type", type)), (context, request) -> {
                            throw new AssertionError("the route must not run");
                        });
                authorization.get().run();

                var answered = response.toCompletableFuture().join();
                assertThat(answered.code()).as(path).isEqualTo(status);
                assertThat(body(answered)).isEqualTo(type == null
                        ? "{\"message\":\"a JSON body is required, with Content-Type application/json\"}"
                        : "{\"message\":\"Content-Type " + type + " is not supported: application/json only\"}");
                assertThat(stderr.text()).contains("outbox admin POST \"/kora-outbox/admin/v1/" + path
                        + "\" by \"ops\": " + status);
            }
            assertThat(stderr.text()).doesNotContain("WARN");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"application/json", "application/json; charset=utf-8", "Application/JSON;charset=UTF-8"})
    void aBodyRouteTakesJson(String type) {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        var response = interceptor.intercept(Context.current(), request("POST",
                "/kora-outbox/admin/v1/dead-letters/discard", HttpHeaders.of("content-type", type)),
                (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(200)));
        authorization.get().run();

        assertThat(response.toCompletableFuture().join().code()).isEqualTo(200);
    }

    /** Clients other than browsers send no such header; a GET changes nothing, so a link from another site works. */
    @ParameterizedTest
    @CsvSource({"POST,", "POST,same-origin", "POST,none", "GET,cross-site"})
    void aCallOfTheSameSiteOrNoBrowserReachesTheAuthorizer(String method, String site) {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        var response = interceptor.intercept(Context.current(), request(method, "/kora-outbox/admin/v1/status",
                site == null ? HttpHeaders.of() : HttpHeaders.of("sec-fetch-site", site)),
                (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(200)));
        authorization.get().run();

        assertThat(response.toCompletableFuture().join().code()).isEqualTo(200);
    }

    private static String body(HttpServerResponse response) {
        var out = new ByteArrayOutputStream();
        try {
            response.body().write(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toString(UTF_8);
    }

    /** Runs the handler only when the test runs what it leaves in {@code run}. */
    private static BlockingRequestExecutor deferred(AtomicReference<Runnable> run) {
        return new BlockingRequestExecutor() {
            @Override
            public <T> CompletionStage<T> execute(Context context, Callable<T> handler) {
                var future = new CompletableFuture<T>();
                run.set(() -> {
                    try {
                        future.complete(handler.call());
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });
                return future;
            }
        };
    }

    private static HttpServerRequest request(String path) {
        return request("GET", path, HttpHeaders.of());
    }

    private static HttpServerRequest request(String method, String path, HttpHeaders headers) {
        return new HttpServerRequest() {
            @Override
            public String method() {
                return method;
            }

            @Override
            public String path() {
                return path;
            }

            @Override
            public String route() {
                return path;
            }

            @Override
            public HttpHeaders headers() {
                return headers;
            }

            @Override
            public List<Cookie> cookies() {
                return List.of();
            }

            @Override
            public Map<String, ? extends Collection<String>> queryParams() {
                return Map.of();
            }

            @Override
            public Map<String, String> pathParams() {
                return Map.of();
            }

            @Override
            public HttpBodyInput body() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
