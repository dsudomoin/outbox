package io.github.dsudomoin.outbox.admin.spring;

import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * A request body as a plain tree, by a mapper of this class's own, Jackson 3, whose default refuses trailing tokens; it
 * refuses a repeated field too, as the default keeps its last value. Loaded only with Jackson 3 present.
 */
final class Jackson3Tree {

    private static final JsonMapper MAPPER =
        JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();

    private Jackson3Tree() {}

    /** Maps, lists and scalars, null for a JSON null; {@link IllegalArgumentException} with the parser's words. */
    static Object read(byte[] body) {
        try {
            return MAPPER.readValue(body, Object.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException(e.getOriginalMessage());
        }
    }
}
