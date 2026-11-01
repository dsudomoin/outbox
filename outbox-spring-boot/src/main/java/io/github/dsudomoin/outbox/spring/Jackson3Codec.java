package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxSerializer;
import java.lang.reflect.Type;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.json.JsonMapper;

/** Over the app's Jackson 3 {@code JsonMapper}. Loaded only with Jackson 3 present. */
final class Jackson3Codec implements JsonCodec {

    private final ObjectProvider<JsonMapper> mappers;

    Jackson3Codec(ObjectProvider<JsonMapper> mappers) {
        this.mappers = mappers;
    }

    @Override
    public OutboxProperties.Json.Mapper major() {
        return OutboxProperties.Json.Mapper.JACKSON3;
    }

    @Override
    public boolean hasMapper() {
        return mappers.stream().findAny().isPresent();
    }

    /** A container's declared type keeps its elements' type ids; a value's own class keeps a subclass's fields. */
    @Override
    public <T> OutboxSerializer<T> serializer(Type type) {
        var mapper = mappers.getObject();
        var javaType = mapper.constructType(type);
        var writer = javaType.isContainerType() ? mapper.writerFor(javaType) : mapper.writer();
        return (topic, value) -> writer.writeValueAsBytes(value);
    }

    @Override
    public <T> OutboxDeserializer<T> deserializer(Type type) {
        var mapper = mappers.getObject();
        var reader = mapper.readerFor(mapper.constructType(type));
        return (topic, payload) -> reader.readValue(payload);
    }
}
