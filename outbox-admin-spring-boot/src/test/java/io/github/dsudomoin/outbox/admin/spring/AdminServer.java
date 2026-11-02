package io.github.dsudomoin.outbox.admin.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/** A whole app on a real Tomcat at a free port, its own outbox table, no relay, called over HTTP. */
final class AdminServer implements AutoCloseable {

    static final String API = "/kora-outbox/admin/v1";
    private static final String AUDIT = " INFO " + OutboxAdminGuard.class.getName() + " - ";

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Base {

        @Bean
        RecordingHandler handler() {
            return new RecordingHandler();
        }
    }

    /** By {@code X-Api-Key}, unless the test puts its own {@link #answer}; remembers the thread of each call. */
    static class Keys implements OutboxAdminAuthorizer {

        final List<String> threads = new CopyOnWriteArrayList<>();
        volatile OutboxAdminAuthorizer answer = request -> {
            var key = request.getHeader("X-Api-Key");
            if (key == null) {
                return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
            }
            return key.equals("secret") ? OutboxAdminAuthorizer.allow("tester")
                : OutboxAdminAuthorizer.forbidden("unknown key");
        };

        @Override
        public Decision authorize(HttpServletRequest request) throws Exception {
            threads.add(Thread.currentThread().getName());
            return answer.authorize(request);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithKeys {

        @Bean
        Keys keys() {
            return new Keys();
        }
    }

    /** A controller's mapping as one call: its method's name, the HTTP method, a path, the type it consumes. */
    record Route(String handler, String method, String path, String contentType) {

        @Override
        public String toString() {
            return handler + " " + method + " " + path;
        }
    }

    final ConfigurableApplicationContext context;
    final TableNames names;
    private final int port;
    private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    private AdminServer(ConfigurableApplicationContext context) {
        this.context = context;
        this.names = TableNames.of(context.getBean(OutboxConfig.class).table());
        this.port = context.getEnvironment().getRequiredProperty("local.server.port", int.class);
    }

    static AdminServer start(List<String> properties, Class<?>... configurations) {
        var all = new HashMap<String, Object>();
        for (var property : AdminHttpIT.properties(properties.toArray(String[]::new))) {
            all.put(property.substring(0, property.indexOf('=')), property.substring(property.indexOf('=') + 1));
        }
        all.put("server.port", "0");
        all.put("spring.main.banner-mode", "off");
        var builder = new SpringApplicationBuilder(Base.class).sources(configurations).properties(all);
        return new AdminServer(builder.run());
    }

    static AdminServer start(Class<?>... configurations) {
        return start(List.of(), configurations);
    }

    @Override
    public void close() {
        context.close();
    }

    <T> T bean(Class<T> type) {
        return context.getBean(type);
    }

    /** Every route of the admin controller, as this app has mapped them; {@code {table}} is {@code x}. */
    List<Route> routes() {
        var routes = new ArrayList<Route>();
        context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class).getHandlerMethods()
            .forEach((info, method) -> {
                if (OutboxAdminController.class.isAssignableFrom(method.getBeanType())) {
                    var type = info.getConsumesCondition().getConsumableMediaTypes().stream().findFirst()
                        .map(Object::toString).orElse(null);
                    for (var http : info.getMethodsCondition().getMethods()) {
                        for (var path : info.getPatternValues()) {
                            routes.add(new Route(method.getMethod().getName(), http.name(),
                                path.replace("{table}", "x"), type));
                        }
                    }
                }
            });
        return routes;
    }

    /** {@code headers} are names and values; a body with no {@code Content-Type} among them goes without one. */
    HttpResponse<String> send(String method, String pathAndQuery, String body, String... headers) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + pathAndQuery))
            .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body));
        if (headers.length > 0) {
            request.headers(headers);
        }
        return client.send(request.build(), BodyHandlers.ofString());
    }

    HttpResponse<String> get(String pathAndQuery, String... headers) throws Exception {
        return send("GET", pathAndQuery, null, headers);
    }

    /** With {@code Content-Type: application/json}. */
    HttpResponse<String> post(String pathAndQuery, String json, String... headers) throws Exception {
        var all = new ArrayList<>(List.of("Content-Type", "application/json"));
        all.addAll(List.of(headers));
        return send("POST", pathAndQuery, json, all.toArray(String[]::new));
    }

    void execute(String sql) throws SQLException {
        try (var c = bean(DataSource.class).getConnection()) {
            TestSql.execute(c, sql);
        }
    }

    static void assertError(HttpResponse<String> response, int status, String message) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(response.body()).isEqualTo("{\"message\":\"" + message + "\"}");
        assertThat(response.headers().firstValue("Content-Type"))
            .hasValueSatisfying(t -> assertThat(t).startsWith("application/json"));
    }

    /** The guard's INFO lines so far, from {@code outbox admin}. */
    static List<String> audit(CapturedStderr stderr) {
        return stderr.text().lines().filter(l -> l.contains(AUDIT))
            .map(l -> l.substring(l.indexOf(AUDIT) + AUDIT.length())).toList();
    }

    /**
     * Waits for {@code lines} of them: MVC flushes an allowed call's response before the guard writes its line, so the
     * line can come just after the client has its response. Call it before the next call, or two lines may swap.
     */
    static List<String> awaitAudit(CapturedStderr stderr, int lines) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (audit(stderr).size() < lines && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        return audit(stderr);
    }
}
