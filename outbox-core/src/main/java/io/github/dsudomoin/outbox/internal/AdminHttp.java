package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
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
        detail.put("ids", ids.stream().limit(AUDIT_IDS).map(String::valueOf)
                .collect(Collectors.joining(", ", "[", ids.size() > AUDIT_IDS ? ", ...]" : "]")));
        return detail;
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
