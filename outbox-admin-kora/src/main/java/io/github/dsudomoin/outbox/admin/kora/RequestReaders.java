package io.github.dsudomoin.outbox.admin.kora;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.github.dsudomoin.outbox.admin.kora.model.AbandonKey;
import io.github.dsudomoin.outbox.admin.kora.model.AbandonWaiting;
import io.github.dsudomoin.outbox.admin.kora.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.kora.model.Filter;
import io.github.dsudomoin.outbox.admin.kora.model.Selection;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import ru.tinkoff.kora.json.common.JsonReader;

/**
 * The request bodies' readers, which refuse a field the contract does not name: the generated ones skip it, so a typo
 * such as {@code eror} would widen a discard to every letter the rest of the filter matches. They refuse a repeated
 * field too: the parser keeps the last value, so {@code "error":"x","error":null} would drop the error from the filter.
 */
final class RequestReaders {

    private RequestReaders() {}

    static JsonReader<Selection> selection(JsonReader<List<Long>> ids, JsonReader<Filter> filter) {
        return parser -> {
            if (!startObject(parser)) {
                return null;
            }
            List<Long> idList = null;
            Filter where = null;
            var seen = new HashSet<String>();
            for (var field = parser.nextFieldName(); field != null; field = parser.nextFieldName()) {
                if (!seen.add(field)) {
                    throw new JsonParseException(parser, "duplicate field '" + field + "' in Selection");
                }
                var isNull = parser.nextToken() == JsonToken.VALUE_NULL;
                switch (field) {
                    case "ids" -> idList = isNull ? null : ids.read(parser);
                    case "filter" -> where = isNull ? null : filter.read(parser);
                    default -> throw unknown(parser, field, "Selection", "ids, filter");
                }
            }
            return new Selection(idList, where);
        };
    }

    static JsonReader<Filter> filter(JsonReader<OffsetDateTime> times, JsonReader<DeadLetterStatus> statuses) {
        return parser -> {
            if (!startObject(parser)) {
                return null;
            }
            String topic = null;
            String key = null;
            String error = null;
            OffsetDateTime failedFrom = null;
            OffsetDateTime failedTo = null;
            DeadLetterStatus status = null;
            var seen = new HashSet<String>();
            for (var field = parser.nextFieldName(); field != null; field = parser.nextFieldName()) {
                if (!seen.add(field)) {
                    throw new JsonParseException(parser, "duplicate field '" + field + "' in Filter");
                }
                var isNull = parser.nextToken() == JsonToken.VALUE_NULL;
                switch (field) {
                    case "topic" -> topic = string(parser, field);
                    case "key" -> key = string(parser, field);
                    case "error" -> error = string(parser, field);
                    case "failedFrom" -> failedFrom = isNull ? null : times.read(parser);
                    case "failedTo" -> failedTo = isNull ? null : times.read(parser);
                    case "status" -> status = isNull ? null : statuses.read(parser);
                    default -> throw unknown(parser, field, "Filter",
                            "topic, key, error, failedFrom, failedTo, status");
                }
            }
            return new Filter(topic, key, error, failedFrom, failedTo, status);
        };
    }

    static JsonReader<AbandonKey> abandonKey() {
        return parser -> {
            if (!startObject(parser)) {
                return null;
            }
            String topic = null;
            String key = null;
            UUID messageId = null;
            String reason = null;
            Boolean dryRun = null;
            var seen = new HashSet<String>();
            for (var field = parser.nextFieldName(); field != null; field = parser.nextFieldName()) {
                if (!seen.add(field)) {
                    throw new JsonParseException(parser, "duplicate field '" + field + "' in AbandonKey");
                }
                parser.nextToken();
                switch (field) {
                    case "topic" -> topic = string(parser, field);
                    case "key" -> key = string(parser, field);
                    case "messageId" -> messageId = uuid(parser, field);
                    case "reason" -> reason = string(parser, field);
                    case "dryRun" -> dryRun = bool(parser, field);
                    default -> throw unknown(parser, field, "AbandonKey", "topic, key, messageId, reason, dryRun");
                }
            }
            if (topic == null || reason == null) {
                throw new JsonParseException(parser, "missing field '" + (topic == null ? "topic" : "reason")
                        + "' in AbandonKey");
            }
            if ((key == null) == (messageId == null)) {
                throw new JsonParseException(parser, "AbandonKey needs exactly one of 'key' and 'messageId'");
            }
            return new AbandonKey(topic, key, messageId, reason, dryRun);
        };
    }

    private static UUID uuid(JsonParser parser, String field) throws IOException {
        var text = string(parser, field);
        try {
            return text == null ? null : UUID.fromString(text);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException(parser, "field '" + field + "' is not a UUID: " + text);
        }
    }

    static JsonReader<AbandonWaiting> abandonWaiting() {
        return parser -> {
            if (!startObject(parser)) {
                return null;
            }
            String topic = null;
            String key = null;
            String reason = null;
            Boolean dryRun = null;
            var seen = new HashSet<String>();
            for (var field = parser.nextFieldName(); field != null; field = parser.nextFieldName()) {
                if (!seen.add(field)) {
                    throw new JsonParseException(parser, "duplicate field '" + field + "' in AbandonWaiting");
                }
                parser.nextToken();
                switch (field) {
                    case "topic" -> topic = string(parser, field);
                    case "key" -> key = string(parser, field);
                    case "reason" -> reason = string(parser, field);
                    case "dryRun" -> dryRun = bool(parser, field);
                    default -> throw unknown(parser, field, "AbandonWaiting", "topic, key, reason, dryRun");
                }
            }
            if (topic == null || reason == null) {
                throw new JsonParseException(parser, "missing field '" + (topic == null ? "topic" : "reason")
                        + "' in AbandonWaiting");
            }
            return new AbandonWaiting(topic, key, reason, dryRun);
        };
    }

    /** False for {@code null}; true at the start of an object. */
    private static boolean startObject(JsonParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) {
            return false;
        }
        if (token != JsonToken.START_OBJECT) {
            throw new JsonParseException(parser, "Expecting START_OBJECT token, got " + token);
        }
        return true;
    }

    @Nullable
    private static String string(JsonParser parser, String field) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_STRING -> parser.getText();
            case VALUE_NULL -> null;
            default -> throw new JsonParseException(parser, "Expecting a string for field '" + field + "', got "
                    + parser.currentToken());
        };
    }

    @Nullable
    private static Boolean bool(JsonParser parser, String field) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_TRUE -> true;
            case VALUE_FALSE -> false;
            case VALUE_NULL -> null;
            default -> throw new JsonParseException(parser, "Expecting a boolean for field '" + field + "', got "
                    + parser.currentToken());
        };
    }

    private static JsonParseException unknown(JsonParser parser, String field, String type, String fields) {
        return new JsonParseException(parser, "unknown field '" + field + "' in " + type + "; its fields: " + fields);
    }
}
