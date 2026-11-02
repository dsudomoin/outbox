package io.github.dsudomoin.outbox.admin.spring;

import io.github.dsudomoin.outbox.admin.spring.model.AbandonWaiting;
import io.github.dsudomoin.outbox.admin.spring.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.spring.model.Filter;
import io.github.dsudomoin.outbox.admin.spring.model.Selection;
import jakarta.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.util.ClassUtils;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdviceAdapter;

/**
 * Reads {@link OutboxAdminController}'s request bodies by the contract, with a mapper of its own, and gives the
 * controller what it read in place of what the app's converter bound: the app's mapper gets no say over an admin
 * request. A String deserializer of the app's, which also serves Jackson's untyped Map, could read {@code "  "} or any
 * other value as absent, and an absent field widens a bulk call to every letter the rest of the filter matches; so can
 * the lenient rest of a mapper: an unknown field skipped (a typo such as {@code eror}), a value coerced
 * ({@code "dryRun":""} to false, {@code [1.5]} to letter 1), a misspelt status read as none. MVC still runs the app's
 * converter, whose result is dropped; if it throws, the call answers 400. As the Kora adapter, this takes a blank
 * {@code topic} or {@code error} of a filter as an error, and an empty {@code key} as the empty key. Bodies stop at
 * {@link #MAX_BODY}, as each is held in memory and parsed twice.
 */
@ControllerAdvice(assignableTypes = OutboxAdminController.class)
class RequestReaders extends RequestBodyAdviceAdapter {

    /** Far above the largest body the contract needs: 1000 ids take about 21 kB. */
    private static final int MAX_BODY = 1024 * 1024;

    private static final Set<String> SELECTION = Set.of("ids", "filter");
    private static final Set<String> FILTER = Set.of("topic", "key", "error", "failedFrom", "failedTo", "status");
    private static final Set<String> ABANDON = Set.of("topic", "key", "reason", "dryRun");

    private static final String BOUND = RequestReaders.class.getName() + ".bound";
    private static final String JACKSON3 = "tools.jackson.databind.json.JsonMapper";
    private static final String JACKSON2 = "com.fasterxml.jackson.databind.ObjectMapper";

    private final Function<byte[], Object> tree = tree();

    /** Named only here and by reference: neither class loads until it reads, so one major is enough. */
    private static Function<byte[], Object> tree() {
        var loader = RequestReaders.class.getClassLoader();
        if (ClassUtils.isPresent(JACKSON3, loader)) {
            return Jackson3Tree::read;
        }
        if (ClassUtils.isPresent(JACKSON2, loader)) {
            return Jackson2Tree::read;
        }
        throw new IllegalStateException("the outbox admin API reads its request bodies with Jackson, and neither "
            + JACKSON3 + " nor " + JACKSON2 + " is on the classpath: add jackson-databind");
    }

    @Override
    public boolean supports(MethodParameter parameter, Type targetType,
            Class<? extends HttpMessageConverter<?>> converterType) {
        return true;
    }

    @Override
    public HttpInputMessage beforeBodyRead(HttpInputMessage input, MethodParameter parameter, Type targetType,
            Class<? extends HttpMessageConverter<?>> converterType) throws IOException {
        var body = input.getBody().readNBytes(MAX_BODY + 1);
        if (body.length > MAX_BODY) {
            throw new IllegalArgumentException("a request body of at most " + MAX_BODY + " bytes, not more");
        }
        var message = new HttpInputMessage() {
            @Override
            public InputStream getBody() {
                return new ByteArrayInputStream(body);
            }

            @Override
            public HttpHeaders getHeaders() {
                return input.getHeaders();
            }
        };
        var read = tree.apply(body);
        if (read != null) { // a JSON null binds nothing: MVC refuses it as a missing body
            if (!(read instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("Expecting a JSON object as the request body");
            }
            RequestContextHolder.currentRequestAttributes().setAttribute(BOUND,
                targetType == Selection.class ? selection(map) : abandonWaiting(map), RequestAttributes.SCOPE_REQUEST);
        }
        return message;
    }

    /** What {@link #beforeBodyRead} made of the body, never what the app's converter did. */
    @Override
    public Object afterBodyRead(Object body, HttpInputMessage input, MethodParameter parameter, Type targetType,
            Class<? extends HttpMessageConverter<?>> converterType) {
        return RequestContextHolder.currentRequestAttributes().getAttribute(BOUND, RequestAttributes.SCOPE_REQUEST);
    }

    private static Selection selection(Map<?, ?> body) {
        known(body, "Selection", SELECTION, "ids, filter");
        var selection = new Selection();
        var ids = body.get("ids");
        if (ids != null) {
            if (!(ids instanceof List<?> list)) {
                throw new IllegalArgumentException("Expecting an array for field 'ids'");
            }
            var longs = new ArrayList<Long>(list.size());
            for (var id : list) {
                if (id != null && !(id instanceof Integer || id instanceof Long)) {
                    throw new IllegalArgumentException("Expecting ids to be 64-bit integers, got " + id);
                }
                longs.add(id == null ? null : ((Number) id).longValue()); // the controller refuses a null
            }
            selection.setIds(longs);
        }
        var filter = body.get("filter");
        if (filter != null) {
            if (!(filter instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("Expecting an object for field 'filter'");
            }
            selection.setFilter(filter(map));
        }
        return selection;
    }

    private static Filter filter(Map<?, ?> body) {
        known(body, "Filter", FILTER, "topic, key, error, failedFrom, failedTo, status");
        var status = string(body, "status");
        return new Filter().topic(notBlank(body, "topic")).key(string(body, "key")).error(notBlank(body, "error"))
            .failedFrom(string(body, "failedFrom")).failedTo(string(body, "failedTo"))
            .status(status == null ? null : DeadLetterStatus.fromValue(status));
    }

    private static AbandonWaiting abandonWaiting(Map<?, ?> body) {
        known(body, "AbandonWaiting", ABANDON, "topic, key, reason, dryRun");
        var topic = string(body, "topic");
        var key = string(body, "key");
        var reason = string(body, "reason");
        if (topic == null || reason == null) {
            throw new IllegalArgumentException("missing field '" + (topic == null ? "topic" : "reason")
                + "' in AbandonWaiting");
        }
        var dryRun = body.get("dryRun");
        if (dryRun != null && !(dryRun instanceof Boolean)) {
            throw new IllegalArgumentException("Expecting a boolean for field 'dryRun', got " + dryRun);
        }
        return new AbandonWaiting(topic, reason).key(key).dryRun((Boolean) dryRun);
    }

    private static void known(Map<?, ?> body, String type, Set<String> fields, String listed) {
        for (var field : body.keySet()) {
            if (!fields.contains(field)) {
                throw new IllegalArgumentException("unknown field '" + field + "' in " + type + "; its fields: "
                    + listed);
            }
        }
    }

    @Nullable
    private static String string(Map<?, ?> body, String field) {
        var value = body.get(field);
        if (value != null && !(value instanceof String)) {
            throw new IllegalArgumentException("Expecting a string for field '" + field + "', got " + value);
        }
        return (String) value;
    }

    /** A filter's {@code topic} or {@code error}: a blank one would match every letter the rest of it matches. */
    @Nullable
    private static String notBlank(Map<?, ?> body, String field) {
        var value = string(body, field);
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException("a blank " + field + " in a filter matches every " + field
                + ": leave it out, or name one");
        }
        return value;
    }
}
