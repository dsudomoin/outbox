package io.github.dsudomoin.outbox.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxSerializer;
import java.lang.reflect.Type;
import org.springframework.beans.factory.ObjectProvider;

/** Over the app's Jackson 2 {@code ObjectMapper}. Loaded only with Jackson 2 present. */
final class Jackson2Codec implements JsonCodec {

    private final ObjectProvider<ObjectMapper> mappers;

    Jackson2Codec(ObjectProvider<ObjectMapper> mappers) {
        this.mappers = mappers;
    }

    @Override
    public OutboxProperties.Json.Mapper major() {
        return OutboxProperties.Json.Mapper.JACKSON2;
    }

    @Override
    public boolean hasMapper() {
        return mappers.stream().findAny().isPresent();
    }

    /**
     * A generic or container type is written as declared, or what it holds would lose its type ids and could never be
     * read back; any other as the value's own class, or a subclass would lose its fields.
     */
    @Override
    public <T> OutboxSerializer<T> serializer(Type type) {
        var mapper = mappers.getObject();
        var javaType = mapper.constructType(type);
        var writer = javaType.isContainerType() || javaType.hasGenericTypes()
            ? mapper.writerFor(javaType) : mapper.writer();
        return (topic, value) -> writer.writeValueAsBytes(value);
    }

    @Override
    public <T> OutboxDeserializer<T> deserializer(Type type) {
        var mapper = mappers.getObject();
        var reader = mapper.readerFor(mapper.constructType(type));
        return (topic, payload) -> reader.readValue(payload);
    }
}
