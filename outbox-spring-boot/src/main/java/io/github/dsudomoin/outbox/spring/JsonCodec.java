package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxSerializer;
import java.lang.reflect.Type;

/** The codecs of one Jackson major over the app's mapper bean; nothing outside the implementations names Jackson. */
interface JsonCodec {

    OutboxProperties.Json.Mapper major();

    /** Whether a mapper bean exists; which of several gets used is decided, or refused, when a codec is first asked. */
    boolean hasMapper();

    <T> OutboxSerializer<T> serializer(Type type);

    <T> OutboxDeserializer<T> deserializer(Type type);
}
