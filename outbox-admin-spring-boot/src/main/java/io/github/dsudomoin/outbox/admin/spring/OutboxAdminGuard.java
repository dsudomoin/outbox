package io.github.dsudomoin.outbox.admin.spring;

import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Guards every admin route with the {@link OutboxAdminAuthorizer}, called on the request's thread. MVC runs this after
 * it has picked the handler and before it resolves the handler's arguments, so before the controller reads a JSON body
 * (a multipart request is another matter: {@code DispatcherServlet.checkMultipart} parses one ahead of the handler
 * lookup, on every path of the app). The decision follows the handler, not the path: it holds wherever an app moves the
 * routes, and it passes anything but {@link OutboxAdminController} at once.
 *
 * <p>Should a handler mapping leave this out (an app's own mapping that keeps the handler alone, or the controller
 * scanned without the auto-configuration), the controller calls {@link #requireAllowed} before anything else of the
 * route, so a call nobody allowed gets a 500 {@code Error} "authorization did not run", a WARN naming the cause and a
 * {@code denied: 500} audit line, never the route.
 *
 * <p>A denied call gets a JSON {@code Error} with the decision's status and never reaches the route; an authorizer that
 * fails gets a WARN with its exception and the caller a 500 {@code Error} "authorization failed", nothing of why. A
 * call other than a {@code GET} or {@code HEAD} that a browser marks as sent by another site gets a 403 {@code Error}
 * "cross-site request refused" without the authorizer: a page elsewhere cannot change state with what the operator's
 * browser adds by itself. The browser's mark is {@code Sec-Fetch-Site} other than {@code same-origin} or {@code none};
 * without that header (a browser sends none over plain HTTP to an address other than localhost) it is an
 * {@code Origin} of {@code null} or of another host and port than the request's server name and port, the scheme
 * aside: those the container took from {@code Host}, or from forwarded headers where the app takes them
 * ({@code server.forward-headers-strategy}). Curl and service clients send neither header. Each is a
 * {@link Refused} that the controller's own handler answers, so neither an app's advice nor the app's JSON library is
 * this class's concern.
 *
 * <p>Every call leaves one INFO audit line: {@code outbox admin <method> "<path>" by "<identity>": <status>} when
 * allowed, {@code outbox admin <method> "<path>" denied: <status>} when denied or when the authorizer fails. An allowed
 * call whose operation put its arguments under {@link OutboxAdminController#DETAIL} has them before the status:
 * {@code by "<identity>" with <name> "<value>", ...}, a null value as {@code null}. The path (decoded and normalized
 * by the container, without the context path, the query or a {@code ;} part), the identity (perhaps a token's subject)
 * and each value are quoted, with their quotes, backslashes, control and format characters escaped, so no caller can
 * forge an audit line or a part of one.
 * The text of an exception an authorizer or a route throws goes to its WARN line as it is.
 *
 * <p>The line of a denial or of a failed authorizer is written here before its response. MVC has already flushed an
 * allowed call's response when it runs {@link #afterCompletion}, so a client can see that response just before its line
 * exists, unlike the Kora adapter's, whose line comes before its response completes. The line is still always written:
 * MVC runs {@link #afterCompletion} for a call {@link #preHandle} let through, whatever became of it.
 */
final class OutboxAdminGuard implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminGuard.class);
    /** Request attribute: who an allowed call is, for its audit line. */
    private static final String IDENTITY = OutboxAdminGuard.class.getName() + ".identity";

    /** A type of its own: an app's filter copying a parameter into the attributes must not pass for an allowed call. */
    private record Allowed(String identity) {}

    private final Supplier<OutboxAdminAuthorizer> authorizer;

    /** A supplier, so that making this guard, which a handler mapping's start does, needs nothing of the app's. */
    OutboxAdminGuard(Supplier<OutboxAdminAuthorizer> authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!guards(handler)) {
            return true;
        }
        var call = call(request);
        var method = request.getMethod();
        if (!method.equals("GET") && !method.equals("HEAD") && sentByAnotherSite(request)) {
            log.info("outbox admin {} denied: 403", call);
            throw new Refused(403, "cross-site request refused");
        }
        OutboxAdminAuthorizer.Decision decision;
        try {
            decision = Objects.requireNonNull(authorizer.get().authorize(request),
                "OutboxAdminAuthorizer returned null");
        } catch (Throwable e) { // an Error too: the call gets its audit line, the caller nothing of why
            log.warn("outbox admin {}: the OutboxAdminAuthorizer failed, answered 500", call, e);
            log.info("outbox admin {} denied: 500", call);
            throw new Refused(500, "authorization failed");
        }
        if (decision instanceof OutboxAdminAuthorizer.Allow allow) {
            request.setAttribute(IDENTITY, new Allowed(allow.identity()));
            return true;
        }
        var deny = (OutboxAdminAuthorizer.Deny) decision;
        log.info("outbox admin {} denied: {}", call, deny.status());
        throw new Refused(deny.status(), deny.message());
    }

    /** Checks the handler too: the attribute outlives the call into the container's error dispatch, if there is one. */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
            @Nullable Exception ex) {
        if (guards(handler) && allowed(request)) {
            log.info("outbox admin {}{}: {}", caller(request), detail(request),
                ex != null ? 500 : response.getStatus());
        }
    }

    /** Behind an app's aspect the bean is a subclass, which {@link HandlerMethod#getBeanType} sees through anyway. */
    private static boolean guards(Object handler) {
        return handler instanceof HandlerMethod method
            && OutboxAdminController.class.isAssignableFrom(method.getBeanType());
    }

    /** Whether this guard let the call through. */
    static boolean allowed(HttpServletRequest request) {
        return request.getAttribute(IDENTITY) instanceof Allowed;
    }

    /** Refuses a call this guard did not let through: whatever lost the guard, it must not open a route. */
    static void requireAllowed(HttpServletRequest request) {
        if (!allowed(request)) {
            log.warn("outbox admin {}: authorization did not run, answered 500: the guard is not on the handler mapping"
                + " that picked this route (an app's own HandlerMapping dropping interceptors, or the controller"
                + " scanned without OutboxAdminAutoConfiguration)", call(request));
            auditDenied(request);
            throw new Refused(500, "authorization did not run");
        }
    }

    /** The audit line of a call refused with a 500 outside {@link #preHandle}. */
    static void auditDenied(HttpServletRequest request) {
        log.info("outbox admin {} denied: 500", call(request));
    }

    /** {@code <method> "<path>"}, with {@code by "<identity>"} once allowed: how a line about this call names it. */
    static String caller(HttpServletRequest request) {
        return call(request) + (request.getAttribute(IDENTITY) instanceof Allowed allowed
            ? " by \"" + printable(allowed.identity()) + "\"" : "");
    }

    private static String call(HttpServletRequest request) {
        var path = request.getServletPath() + Objects.requireNonNullElse(request.getPathInfo(), "");
        return request.getMethod() + " \"" + printable(path) + "\"";
    }

    /**
     * Whether a browser says another site made it send {@code request} ({@code Sec-Fetch-Site}): such a request may
     * carry what the browser adds by itself (a cookie, an SSO session, the operator's network address), which an
     * authorizer cannot tell from the operator's own call. Without that header, which a browser sends only to a
     * trustworthy origin (HTTPS or localhost), an {@code Origin} that is not this server's host and port says so.
     * Clients other than browsers send neither header.
     */
    private static boolean sentByAnotherSite(HttpServletRequest request) {
        var site = request.getHeader("Sec-Fetch-Site");
        if (site != null) {
            return !site.equals("same-origin") && !site.equals("none");
        }
        var origin = request.getHeader("Origin");
        return origin != null && !sameHost(origin, request.getServerName(), port(request));
    }

    /**
     * The port the browser named, below 0 for its scheme's default: a container fills a {@code Host} with no port in
     * with the default of its own scheme, which behind a proxy ending TLS is 80 for a page on 443. Another port came
     * from the {@code Host} or from forwarded headers the app takes, and is the one compared.
     */
    private static int port(HttpServletRequest request) {
        var port = request.getServerPort();
        var host = request.getHeader("Host");
        var named = host != null && host.lastIndexOf(':') > host.lastIndexOf(']'); // an IPv6 literal has ':' inside
        return !named && port == (request.isSecure() ? 443 : 80) ? -1 : port;
    }

    /**
     * Whether {@code origin} names {@code host} and {@code port} (below 0: the default of the browser's scheme). Not
     * the scheme: behind a proxy ending TLS the page had https, the server http.
     */
    private static boolean sameHost(String origin, String host, int port) {
        try {
            var page = new URI(origin);
            var defaultPort = "https".equalsIgnoreCase(page.getScheme()) ? 443 : 80;
            return page.getHost() != null
                && page.getHost().toLowerCase(Locale.ROOT).equals(host.toLowerCase(Locale.ROOT))
                && (page.getPort() < 0 ? defaultPort : page.getPort()) == (port < 0 ? defaultPort : port);
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /** {@code  with <name> "<value>", ...} of the operation's detail, each value {@link #printable}; else empty. */
    private static String detail(HttpServletRequest request) {
        if (!(request.getAttribute(OutboxAdminController.DETAIL) instanceof Map<?, ?> detail)) {
            return "";
        }
        var out = new StringJoiner(", ", " with ", "");
        detail.forEach((name, value) -> out.add(name + " "
            + (value == null ? "null" : "\"" + printable(value.toString()) + "\"")));
        return out.toString();
    }

    /**
     * {@code text} with quotes and backslashes escaped and each control or format character (a bidi override reorders
     * a line on screen) as its Java escape, so it stays one quoted token on one log line.
     */
    private static String printable(String text) {
        var out = new StringBuilder(text.length());
        text.codePoints().forEach(ch -> {
            switch (ch) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (Character.isISOControl(ch) || ch == '\u2028' || ch == '\u2029'
                        || Character.getType(ch) == Character.FORMAT) {
                        for (var unit : Character.toChars(ch)) { // beyond U+FFFF, Java's escape is two of them
                            out.append(String.format("\\u%04x", (int) unit));
                        }
                    } else {
                        out.appendCodePoint(ch);
                    }
                }
            }
        });
        return out.toString();
    }

    /** What the guard answers instead of the route; its line is written, so its handler only answers. */
    static final class Refused extends RuntimeException {

        final int status;

        Refused(int status, String message) {
            super(message, null, false, false);
            this.status = status;
        }
    }
}
