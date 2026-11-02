package io.github.dsudomoin.outbox.admin.spring;

import io.github.dsudomoin.outbox.admin.spring.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.spring.model.Selection;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdviceAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

/**
 * Checks {@link OutboxAdminController}'s request bodies against the contract before they are bound, as the app's
 * lenient mapper would not: an unknown field it skips, so a typo such as {@code eror} would widen a discard to every
 * letter the rest of the filter matches, and values it coerces ({@code "dryRun":""} to false, {@code [1.5]} to letter
 * 1, with {@code read-unknown-enum-values-as-null} a misspelt status to none). Reads the body as a plain tree with the
 * converter about to bind it, so with the app's own JSON library, whichever Jackson that is, without naming one; what
 * this passes has only the contract's fields, each of the contract's JSON type and enum values, which leaves a mapper's
 * settings nothing to coerce. Bodies stop at {@link #MAX_BODY}, as each is held in memory and parsed twice.
 */
@ControllerAdvice(assignableTypes = OutboxAdminController.class)
class RequestReaders extends RequestBodyAdviceAdapter {

    /** Far above the largest body the contract needs: 1000 ids take about 21 kB. */
    private static final int MAX_BODY = 1024 * 1024;

    private static final Set<String> SELECTION = Set.of("ids", "filter");
    private static final Set<String> FILTER = Set.of("topic", "key", "error", "failedFrom", "failedTo", "status");
    private static final Set<String> ABANDON = Set.of("topic", "key", "reason", "dryRun");

    private final ObjectProvider<RequestMappingHandlerAdapter> adapters;

    RequestReaders(ObjectProvider<RequestMappingHandlerAdapter> adapters) {
        this.adapters = adapters;
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
        var tree = converter(converterType).read(Map.class, message);
        if (tree != null) { // a JSON null: MVC refuses it as a missing body
            if (targetType == Selection.class) {
                selection((Map<?, ?>) tree);
            } else {
                abandonWaiting((Map<?, ?>) tree);
            }
        }
        return message;
    }

    /** The instance MVC picked: the same class, in the same list, first. */
    @SuppressWarnings("unchecked")
    private HttpMessageConverter<Object> converter(Class<? extends HttpMessageConverter<?>> type) {
        return (HttpMessageConverter<Object>) adapters.orderedStream().flatMap(a -> a.getMessageConverters().stream())
            .filter(type::isInstance).findFirst().orElseThrow();
    }

    private static void selection(Map<?, ?> body) {
        known(body, "Selection", SELECTION, "ids, filter");
        var ids = body.get("ids");
        if (ids != null) {
            if (!(ids instanceof List<?> list)) {
                throw new IllegalArgumentException("Expecting an array for field 'ids'");
            }
            for (var id : list) {
                if (id != null && !(id instanceof Integer || id instanceof Long)) {
                    throw new IllegalArgumentException("Expecting ids to be 64-bit integers, got " + id);
                }
            }
        }
        var filter = body.get("filter");
        if (filter != null) {
            if (!(filter instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("Expecting an object for field 'filter'");
            }
            known(map, "Filter", FILTER, "topic, key, error, failedFrom, failedTo, status");
            FILTER.forEach(field -> string(map, field));
            if (map.get("status") instanceof String status) {
                DeadLetterStatus.fromValue(status); // refuses what a mapper may read as absent
            }
        }
    }

    private static void abandonWaiting(Map<?, ?> body) {
        known(body, "AbandonWaiting", ABANDON, "topic, key, reason, dryRun");
        for (var field : List.of("topic", "key", "reason")) {
            string(body, field);
        }
        for (var field : List.of("topic", "reason")) {
            if (body.get(field) == null) {
                throw new IllegalArgumentException("missing field '" + field + "' in AbandonWaiting");
            }
        }
        var dryRun = body.get("dryRun");
        if (dryRun != null && !(dryRun instanceof Boolean)) {
            throw new IllegalArgumentException("Expecting a boolean for field 'dryRun', got " + dryRun);
        }
    }

    private static void known(Map<?, ?> body, String type, Set<String> fields, String listed) {
        for (var field : body.keySet()) {
            if (!fields.contains(field)) {
                throw new IllegalArgumentException("unknown field '" + field + "' in " + type + "; its fields: "
                    + listed);
            }
        }
    }

    private static void string(Map<?, ?> body, String field) {
        var value = body.get(field);
        if (value != null && !(value instanceof String)) {
            throw new IllegalArgumentException("Expecting a string for field '" + field + "', got " + value);
        }
    }
}
