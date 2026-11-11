package io.github.dsudomoin.outbox.admin.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A request body as a plain tree, by a mapper of this class's own, Jackson 2, which refuses trailing tokens and a
 * repeated field: the default keeps its last value, so {@code "error":null} after {@code "error":"x"} would drop it.
 * Loaded only with Jackson 2 present.
 */
final class Jackson2Tree {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .build();

    private Jackson2Tree() {}

    /** Maps, lists and scalars, null for a JSON null; {@link IllegalArgumentException} with the parser's words. */
    static Object read(byte[] body) {
        try {
            return MAPPER.readValue(body, Object.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getOriginalMessage());
        } catch (IOException e) { // an array in memory has no other failure
            throw new UncheckedIOException(e);
        }
    }
}
