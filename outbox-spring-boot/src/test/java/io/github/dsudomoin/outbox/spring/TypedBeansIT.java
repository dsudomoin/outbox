package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxSerializer;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/** {@code TypedOutbox<T>}, {@code OutboxSerializer<T>} and {@code OutboxDeserializer<T>} served by T at injection. */
class TypedBeansIT {

    private static final Duration WAIT = Duration.ofSeconds(10);
    /** Without these neither Boot line declares a mapper; a class absent from one line is no error for exclude. */
    private static final String NO_JACKSON2 = "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration,"
        + "org.springframework.boot.jackson2.autoconfigure.Jackson2AutoConfiguration";
    private static final String NO_JACKSON3 = "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration";
    private static final String NO_MAPPER_BEANS = "spring.autoconfigure.exclude=" + NO_JACKSON2 + "," + NO_JACKSON3;
    private static final String NO_JACKSON2_BEAN = "spring.autoconfigure.exclude=" + NO_JACKSON2;
    private static final OrderCreated ORDER = new OrderCreated(1, "u1", Instant.parse("2026-11-02T10:15:30Z"));

    public record OrderCreated(long id, String userId, Instant at) {}

    public record Refund(long orderId) {}

    @OutboxContract(2)
    public record OrderCreatedV2(long id) {}

    /** Takes its deserializer the way an app's handler does. */
    static class OrdersHandler extends TypedOutboxTopicHandler<OrderCreated> {

        final List<OrderCreated> received = new CopyOnWriteArrayList<>();

        OrdersHandler(OutboxDeserializer<OrderCreated> deserializer) {
            super(Set.of("orders"), deserializer);
        }

        @Override
        protected void handle(OrderCreated value, OutboxRecord record) {
            received.add(value);
        }
    }

    static class Orders {

        final TypedOutbox<OrderCreated> events;

        Orders(TypedOutbox<OrderCreated> events) {
            this.events = events;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import({Orders.class, OrdersHandler.class})
    static class OrdersApp {}

    @Configuration(proxyBeanMethods = false)
    static class Jackson3Bean {

        /** Snake case tells its payloads from Jackson 2's. */
        @Bean
        JsonMapper jackson3() {
            return JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build();
        }
    }

    /** Claims what no test here sends typed, so that every app has somewhere to deliver. */
    static final class AnyRoute implements OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("any");
        }

        @Override
        public void handle(OutboxRecord record) {}
    }

    private static ApplicationContextRunner app(String... properties) {
        return runner(properties).withUserConfiguration(AnyRoute.class);
    }

    private static void sendAndReceive(ApplicationContext context) {
        new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
            .executeWithoutResult(status -> context.getBean(Orders.class).events.send("orders", "k", ORDER));
        await().atMost(WAIT).untilAsserted(() ->
            assertThat(context.getBean(OrdersHandler.class).received).containsExactly(ORDER));
    }

    private static String payload(TypedOutbox<OrderCreated> events) {
        return new String(events.message("orders", "k", ORDER).payload(), UTF_8);
    }

    private static String failure(AssertableApplicationContext context) {
        return HandlerProxiesIT.failure(context);
    }

    @Test
    void typedSendAndHandlerJackson2() {
        app("outbox.json.mapper=jackson2").withUserConfiguration(OrdersApp.class).run(context -> {
            assertThat(payload(context.getBean(Orders.class).events)).contains("\"userId\":\"u1\"");
            sendAndReceive(context);
        });
    }

    @Test
    void typedSendAndHandlerJackson3() {
        app("outbox.json.mapper=jackson3").withUserConfiguration(OrdersApp.class, Jackson3Bean.class)
            .run(context -> {
                assertThat(payload(context.getBean(Orders.class).events)).contains("\"user_id\":\"u1\"");
                sendAndReceive(context);
            });
    }

    @Test
    void autoPrefersJackson2WhenBothExist() {
        app().withUserConfiguration(OrdersApp.class, Jackson3Bean.class).run(context -> {
            assertThat(context.getBeanNamesForType(ObjectMapper.class)).isNotEmpty();
            assertThat(payload(context.getBean(Orders.class).events)).contains("\"userId\":\"u1\"");
            sendAndReceive(context);
        });
    }

    @Test
    void autoTakesJackson3WithoutAnObjectMapper() {
        app(NO_JACKSON2_BEAN).withUserConfiguration(OrdersApp.class, Jackson3Bean.class).run(context -> {
            assertThat(context.getBeanNamesForType(ObjectMapper.class)).isEmpty();
            assertThat(payload(context.getBean(Orders.class).events)).contains("\"user_id\":\"u1\"");
            sendAndReceive(context);
        });
    }

    @Test
    void namedMapperMissingFails() {
        app("outbox.json.mapper=jackson3", NO_MAPPER_BEANS).run(context ->
            assertThat(failure(context)).contains("outbox.json.mapper=jackson3", JsonMapper.class.getName()));
        app("outbox.json.mapper=jackson2", NO_MAPPER_BEANS).run(context ->
            assertThat(failure(context)).contains("outbox.json.mapper=jackson2", ObjectMapper.class.getName()));
    }

    @Test
    void namedMapperMissingFailsUnderGlobalLazyInit() {
        app("outbox.json.mapper=jackson3", NO_MAPPER_BEANS)
            .withBean(LazyInitializationBeanFactoryPostProcessor.class)
            .run(context -> assertThat(failure(context)).contains("outbox.json.mapper=jackson3"));
    }

    @Test
    void namedMapperWithoutItsLibraryFails() {
        app("outbox.json.mapper=jackson3", NO_MAPPER_BEANS).withClassLoader(new FilteredClassLoader("tools.jackson"))
            .run(context -> assertThat(failure(context)).contains("outbox.json.mapper=jackson3"));
    }

    /** The app's own OutboxConfig leaves no OutboxProperties bean: the key still counts. */
    @Test
    void namedMapperCountsWithAnOwnOutboxConfig() {
        app("outbox.json.mapper=jackson3").withUserConfiguration(OrdersApp.class, Jackson3Bean.class)
            .withBean(TestOutboxConfig.class)
            .run(context -> assertThat(payload(context.getBean(Orders.class).events)).contains("\"user_id\":\"u1\""));
    }

    @Test
    void noMapperFailsATypedInjectionNamingTheType() {
        app(NO_MAPPER_BEANS).withUserConfiguration(OrdersApp.class).run(context -> {
            assertThat(failure(context)).contains(OutboxDeserializer.class.getName(), OrderCreated.class.getName());
        });
    }

    /** With no mapper, a typed injection is all that fails: an explicit codec needs none. */
    @Test
    void noMapperLeavesExplicitCodecsWorking() {
        app(NO_MAPPER_BEANS).withUserConfiguration(Orders.class, ExplicitSerializer.class).run(context ->
            assertThat(payload(context.getBean(Orders.class).events)).isEqualTo("explicit"));
    }

    @Test
    void noMapperAnswersAnOptionalInjectionWithNothing() {
        app(NO_MAPPER_BEANS).withBean(OptionalCodec.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(OptionalCodec.class).deserializers.getIfAvailable()).isNull();
        });
    }

    static class OptionalCodec {

        final ObjectProvider<OutboxDeserializer<OrderCreated>> deserializers;

        OptionalCodec(ObjectProvider<OutboxDeserializer<OrderCreated>> deserializers) {
            this.deserializers = deserializers;
        }
    }

    /** An app without Jackson or protobuf: the auto-configuration starts, and typed injections say what is missing. */
    @Test
    void noOptionalLibraries() {
        var absent = new FilteredClassLoader("com.fasterxml.jackson", "tools.jackson", "com.google.protobuf");
        app().withClassLoader(absent).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(ProtobufOutbox.class);
            assertThat(context.getBean(OutboxLifecycle.class).isRunning()).isTrue();
        });
        app().withClassLoader(absent).withUserConfiguration(Orders.class).run(context ->
            assertThat(failure(context)).contains(OutboxSerializer.class.getName(), OrderCreated.class.getName()));
    }

    @Configuration(proxyBeanMethods = false)
    static class ExplicitSerializer {

        @Bean
        OutboxSerializer<OrderCreated> explicitSerializer() {
            return (topic, value) -> "explicit".getBytes(UTF_8);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ExplicitDeserializer {

        @Bean
        OutboxDeserializer<OrderCreated> explicitDeserializer() {
            return (topic, payload) -> ORDER;
        }
    }

    static class Refunds {

        final TypedOutbox<Refund> events;
        final OutboxSerializer<OrderCreated> orders;

        Refunds(TypedOutbox<Refund> events, OutboxSerializer<OrderCreated> orders) {
            this.events = events;
            this.orders = orders;
        }
    }

    @Test
    void explicitSerializerWins() {
        app().withUserConfiguration(Orders.class, Refunds.class, ExplicitSerializer.class).run(context -> {
            assertThat(payload(context.getBean(Orders.class).events)).isEqualTo("explicit");
            var refunds = context.getBean(Refunds.class);
            assertThat(refunds.orders).isSameAs(context.getBean("explicitSerializer"));
            assertThat(new String(refunds.events.message("refunds", "k", new Refund(7)).payload(), UTF_8))
                .as("other types still get JSON").isEqualTo("{\"orderId\":7}");
        });
    }

    @Test
    void explicitDeserializerWins() {
        app().withUserConfiguration(OrdersHandler.class, ExplicitDeserializer.class).run(context -> {
            var handler = context.getBean(OrdersHandler.class);
            handler.handle(new OutboxRecord(1, UUID.randomUUID(), "orders", null, "garbage".getBytes(UTF_8),
                List.of(), Instant.now(), 1, 0));
            assertThat(handler.received).containsExactly(ORDER);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class GenericSerializer {

        @Bean
        <T> OutboxSerializer<T> appGenericSerializer() {
            return (topic, value) -> "generic".getBytes(UTF_8);
        }
    }

    /** An app's generic serializer is no ambiguity with the auto-configured one, both matching in the fallback pass. */
    @Test
    void explicitGenericSerializerWins() {
        app().withUserConfiguration(Orders.class, GenericSerializer.class).run(context ->
            assertThat(payload(context.getBean(Orders.class).events)).isEqualTo("generic"));
    }

    @Configuration(proxyBeanMethods = false)
    static class ExplicitTypedOutbox {

        @Bean
        TypedOutbox<OrderCreated> explicitOrders(Outbox outbox) {
            return new TypedOutbox<>(outbox, (topic, value) -> "own".getBytes(UTF_8), OrderCreated.class);
        }
    }

    @Test
    void explicitTypedOutboxWins() {
        app().withUserConfiguration(Orders.class, Refunds.class, ExplicitTypedOutbox.class).run(context -> {
            assertThat(context.getBean(Orders.class).events).isSameAs(context.getBean("explicitOrders"));
            assertThat(new String(context.getBean(Refunds.class).events.message("r", "k", new Refund(7)).payload(),
                UTF_8)).isEqualTo("{\"orderId\":7}");
        });
    }

    static class V2Handler extends TypedOutboxTopicHandler<OrderCreatedV2> {

        final RecordingHandler records = new RecordingHandler();

        V2Handler(OutboxDeserializer<OrderCreatedV2> deserializer) {
            super(Set.of("orders-v2"), deserializer);
        }

        @Override
        protected void handle(OrderCreatedV2 value, OutboxRecord record) throws Exception {
            records.handle(record);
        }
    }

    static class V2Sender {

        final TypedOutbox<OrderCreatedV2> events;

        V2Sender(TypedOutbox<OrderCreatedV2> events) {
            this.events = events;
        }
    }

    @Test
    void contractHeaderStamped() {
        app().withUserConfiguration(V2Handler.class, V2Sender.class).run(context -> {
            new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).executeWithoutResult(status ->
                context.getBean(V2Sender.class).events.send("orders-v2", "k", new OrderCreatedV2(1)));
            var records = context.getBean(V2Handler.class).records;
            await().atMost(WAIT).until(() -> !records.delivered().isEmpty());
            var delivered = records.delivered().get(0);
            assertThat(delivered.contractVersion()).isEqualTo(2);
            assertThat(delivered.headers()).contains(new OutboxHeader("outbox-contract", "2".getBytes(UTF_8)));
        });
    }

    static class RawOrders {

        @SuppressWarnings("rawtypes")
        RawOrders(TypedOutbox events) {}
    }

    @Test
    void rawTypedOutboxInjectionFails() {
        app().withUserConfiguration(RawOrders.class).run(context ->
            assertThat(failure(context)).contains("TypedOutbox", RawOrders.class.getName()));
    }

    static class RawDeserializer {

        @SuppressWarnings("rawtypes")
        @Autowired
        OutboxDeserializer deserializer;
    }

    @Test
    void rawDeserializerInjectionFails() {
        app().withUserConfiguration(RawDeserializer.class).run(context ->
            assertThat(failure(context)).contains("OutboxDeserializer", RawDeserializer.class.getName(),
                "deserializer"));
    }

    @Test
    void protobufOutboxPresent() {
        app().run(context -> {
            var protobuf = context.getBean(ProtobufOutbox.class);
            var id = new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).execute(status ->
                protobuf.send("any", "k", StringValue.of("x")));
            assertThat(id).isNotNull();
        });
    }

    /** A generic payload decodes to its elements, not to the mapper's maps. */
    public static class GenericPayloads {

        final TypedOutbox<List<Refund>> events;
        final OutboxDeserializer<List<Refund>> deserializer;

        GenericPayloads(TypedOutbox<List<Refund>> events, OutboxDeserializer<List<Refund>> deserializer) {
            this.events = events;
            this.deserializer = deserializer;
        }
    }

    @Test
    void genericPayloadKeepsItsElementType() {
        app().withUserConfiguration(GenericPayloads.class).run(context -> {
            var beans = context.getBean(GenericPayloads.class);
            var payload = beans.events.message("t", "k", List.of(new Refund(7))).payload();
            assertThat(beans.deserializer.deserialize("t", payload)).containsExactly(new Refund(7));
        });
        app("outbox.json.mapper=jackson3").withUserConfiguration(GenericPayloads.class, Jackson3Bean.class)
            .run(context -> {
                var beans = context.getBean(GenericPayloads.class);
                var payload = beans.events.message("t", "k", List.of(new Refund(7))).payload();
                assertThat(beans.deserializer.deserialize("t", payload)).containsExactly(new Refund(7));
            });
    }

    abstract static class Sender<T> {

        @Autowired
        TypedOutbox<T> events;
    }

    static class RefundSender extends Sender<Refund> {}

    @Test
    void typeArgumentResolvedThroughASubclass() {
        app().withUserConfiguration(RefundSender.class).run(context ->
            assertThat(new String(context.getBean(RefundSender.class).events
                .message("t", "k", new Refund(7)).payload(), UTF_8)).isEqualTo("{\"orderId\":7}"));
    }

    /** A handler reaches its outbox lazily (an Outbox is built from the handlers): the provider keeps T. */
    static class LazySender {

        final ObjectProvider<TypedOutbox<Refund>> events;

        LazySender(ObjectProvider<TypedOutbox<Refund>> events) {
            this.events = events;
        }
    }

    @Test
    void providerInjectionKeepsTheType() {
        app().withUserConfiguration(LazySender.class).run(context ->
            assertThat(new String(context.getBean(LazySender.class).events.getObject()
                .message("t", "k", new Refund(7)).payload(), UTF_8)).isEqualTo("{\"orderId\":7}"));
    }
}
