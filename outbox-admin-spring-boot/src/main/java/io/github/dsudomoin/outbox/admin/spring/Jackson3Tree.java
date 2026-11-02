package io.github.dsudomoin.outbox.admin.spring;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * A request body as a plain tree, by a mapper of this class's own, Jackson 3, whose default refuses trailing tokens.
 * Loaded only with Jackson 3 present.
 */
final class Jackson3Tree {

    private static final JsonMapper MAPPER = new JsonMapper();

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
