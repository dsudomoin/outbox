package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * What the Kora and Spring admin adapters share: the rules of a dead-letter selection, the detail their audit lines
 * name, its escaping and the Origin check.
 */
public final class AdminHttp {

    /** As the contract's {@code maxItems}: the library binds every id in each chunk's query. */
    public static final int MAX_IDS = 1000;
    /** The ids of a call its audit line shows, after their count: the line stays short for a thousand of them. */
    public static final int AUDIT_IDS = 10;

    private AdminHttp() {}

    /** A selection's ids, or null for its filter ({@code filterGiven}): exactly one of them. */
    @Nullable
    public static List<Long> ids(@Nullable List<Long> ids, boolean filterGiven) {
        if ((ids == null) != filterGiven) {
            throw new IllegalArgumentException("give exactly one of ids and filter");
        }
        if (ids != null && ids.size() > MAX_IDS) {
            throw new IllegalArgumentException("at most " + MAX_IDS + " ids, not " + ids.size()
                    + ": select more with a filter");
        }
        if (ids != null && ids.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("ids must be numbers, not null");
        }
        return ids;
    }

    /** The audit detail of {@code ids}: their count and the first {@value #AUDIT_IDS} of them. */
    public static Map<String, String> idsDetail(List<Long> ids) {
        var detail = new LinkedHashMap<String, String>();
        detail.put("count", String.valueOf(ids.size()));
        detail.put("ids", firstIds(ids));
        return detail;
    }

    /** {@code [<the first {@value #AUDIT_IDS} ids>, ...]}: a thousand ids would make a log line huge. */
    public static String firstIds(Collection<Long> ids) {
        return ids.stream().limit(AUDIT_IDS).map(String::valueOf)
                .collect(Collectors.joining(", ", "[", ids.size() > AUDIT_IDS ? ", ...]" : "]"));
    }

    /** The audit detail of a bulk call's filter, with the key and the status as the caller gave them. */
    public static Map<String, String> filterDetail(DeadLetterFilter filter, @Nullable String key,
            @Nullable String status) {
        var detail = new LinkedHashMap<String, String>();
        detail.put("topic", filter.topic());
        detail.put("key", key);
        detail.put("error", filter.errorContains());
        detail.put("failedFrom", filter.failedFrom() == null ? null : filter.failedFrom().toString());
        detail.put("failedTo", filter.failedTo() == null ? null : filter.failedTo().toString());
        detail.put("status", status);
        return detail;
    }

    /** {@code  with <name> "<value>", ...} of an operation's detail, each value {@link #printable}; empty without it. */
    public static String detail(@Nullable Map<?, ?> detail) {
        if (detail == null) {
            return "";
        }
        var out = new StringJoiner(", ", " with ", "");
        detail.forEach((name, value) -> out.add(name + " "
                + (value == null ? "null" : "\"" + printable(value.toString()) + "\"")));
        return out.toString();
    }

    /**
     * {@code text} with quotes and backslashes escaped, then {@link PgOutboxAdmin#printable}'s control and format
     * escapes, so it stays one quoted token on one log line.
     */
    public static String printable(String text) {
        return PgOutboxAdmin.printable(text.replace("\\", "\\\\").replace("\"", "\\\""));
    }

    /**
     * Whether a browser says another site made it send a request other than GET or HEAD ({@code Sec-Fetch-Site}): such
     * a request may carry what the browser adds by itself (a cookie, an SSO session, the operator's network address),
     * which an authorizer cannot tell from the operator's own call. Without that header, which a browser sends only to
     * a trustworthy origin (HTTPS or localhost), an {@code Origin} that is not this server's host and port says so
     * ({@code sameServer}). Clients other than browsers send neither header. {@code checkOrigin} off leaves
     * {@code Origin} unread.
     */
    public static boolean crossSite(String method, @Nullable String fetchSite, boolean checkOrigin,
            @Nullable String origin, Predicate<String> sameServer) {
        if (method.equals("GET") || method.equals("HEAD")) {
            return false;
        }
        if (fetchSite != null) {
            return !fetchSite.equals("same-origin") && !fetchSite.equals("none");
        }
        return checkOrigin && origin != null && !sameServer.test(origin);
    }

    /** {@code value}, refused when blank: a blank {@code field} of a bulk call's filter would match every letter. */
    @Nullable
    public static String notBlank(String field, @Nullable String value) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException("a blank " + field + " in a filter matches every " + field
                    + ": leave it out, or name one");
        }
        return value;
    }

    /**
     * Whether {@code origin} names {@code host} and {@code port} (below 0: the default of the browser's scheme). Not
     * the scheme: behind a proxy ending TLS the page had https, the server http.
     */
    public static boolean sameHost(String origin, String host, int port) {
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
}
