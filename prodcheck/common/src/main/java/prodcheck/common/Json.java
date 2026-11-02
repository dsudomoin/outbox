package prodcheck.common;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/** Enough JSON for the control endpoints' answers. */
public final class Json {

    private Json() {}

    public static String write(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            return map.entrySet().stream()
                    .map(e -> write(String.valueOf(e.getKey())) + ":" + write(e.getValue()))
                    .collect(Collectors.joining(",", "{", "}"));
        }
        if (value instanceof Collection<?> list) {
            return list.stream().map(Json::write).collect(Collectors.joining(",", "[", "]"));
        }
        var out = new StringBuilder("\"");
        for (var c : value.toString().toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }
}
