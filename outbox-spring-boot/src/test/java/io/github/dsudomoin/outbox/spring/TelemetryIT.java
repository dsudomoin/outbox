package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.properties;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.micrometer.DefaultOutboxDeliveryObservationConvention;
import io.github.dsudomoin.outbox.micrometer.DefaultOutboxEnqueueObservationConvention;
import io.github.dsudomoin.outbox.micrometer.DefaultOutboxSettleObservationConvention;
import io.github.dsudomoin.outbox.micrometer.DefaultOutboxStepObservationConvention;
import io.github.dsudomoin.outbox.micrometer.OutboxDeliveryObservationConvention;
import io.github.dsudomoin.outbox.micrometer.OutboxDeliveryTracingObservationHandler;
import io.github.dsudomoin.outbox.micrometer.OutboxDeliveryContext;
import io.github.dsudomoin.outbox.micrometer.OutboxEnqueueObservationConvention;
import io.github.dsudomoin.outbox.micrometer.OutboxSettleObservationConvention;
import io.github.dsudomoin.outbox.micrometer.OutboxStepObservationConvention;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.otel.bridge.OtelBaggageManager;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/** The telemetry bean from what the context holds: Boot's registries and tracing handlers, the app's own beans. */
class TelemetryIT {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String UNSAMPLED = "test.unsampled=true";
    private static final String SINCE_SUCCESS = "outbox.maintenance.since_success";

    @Configuration(proxyBeanMethods = false)
    static class Metered {

        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    /** The OpenTelemetry SDK with an in-memory exporter behind the bridge; the tracing handlers are Boot's own. */
    @Configuration(proxyBeanMethods = false)
    static class Traced {

        @Bean
        InMemorySpanExporter spans() {
            return InMemorySpanExporter.create();
        }

        @Bean
        SdkTracerProvider sdkTracerProvider(InMemorySpanExporter spans, Environment environment) {
            var sampler = environment.getProperty("test.unsampled", Boolean.class, false)
                ? Sampler.alwaysOff() : Sampler.alwaysOn();
            return SdkTracerProvider.builder().setSampler(sampler).addSpanProcessor(SimpleSpanProcessor.create(spans))
                .build();
        }

        @Bean
        OtelTracer tracer(SdkTracerProvider provider) {
            var current = new OtelCurrentTraceContext();
            return new OtelTracer(provider.get("test"), current, event -> {},
                new OtelBaggageManager(current, List.of(), List.of()));
        }

        @Bean
        OtelPropagator propagator(SdkTracerProvider provider) {
            return new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                provider.get("test"));
        }
    }

    /** The route of topic {@code orders}: the current span of each handle, and each record's traceparent. */
    static final class SpanSeeingRoute implements OutboxTopicHandler {

        private final Tracer tracer;
        final List<String> spanIds = new CopyOnWriteArrayList<>();
        final List<String> traceparents = new CopyOnWriteArrayList<>();

        SpanSeeingRoute(Tracer tracer) {
            this.tracer = tracer;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) {
            spanIds.add(tracer.currentSpan().context().spanId());
            record.headers().stream().filter(h -> h.name().equals("traceparent"))
                .forEach(h -> traceparents.add(new String(h.value(), UTF_8)));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithPoisonRoute {

        @Bean
        OutboxTopicHandler poisonRoute() {
            return new OutboxTopicHandler() {
                @Override
                public Set<String> topics() {
                    return Set.of("poison");
                }

                @Override
                public void handle(OutboxRecord record) {
                    throw new OutboxPermanentException("poison");
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RenamingConventions {

        @Bean
        OutboxEnqueueObservationConvention enqueueConvention() {
            return new DefaultOutboxEnqueueObservationConvention() {
                @Override
                public String getName() {
                    return "custom.enqueue";
                }
            };
        }

        @Bean
        OutboxDeliveryObservationConvention deliveryConvention() {
            return new DefaultOutboxDeliveryObservationConvention() {
                @Override
                public String getName() {
                    return "custom.deliver";
                }
            };
        }

        @Bean
        OutboxStepObservationConvention stepConvention() {
            return new DefaultOutboxStepObservationConvention() {
                @Override
                public String getName() {
                    return "custom.step";
                }
            };
        }

        @Bean
        OutboxSettleObservationConvention settleConvention() {
            return new DefaultOutboxSettleObservationConvention() {
                @Override
                public String getName() {
                    return "custom.settle";
                }
            };
        }
    }

    /** The app's own delivery handler as the README describes it: a subclass, ordered ahead of Boot's handlers. */
    @Configuration(proxyBeanMethods = false)
    static class OwnDeliveryHandler {

        @Bean
        @Order(999)
        OutboxDeliveryTracingObservationHandler ownDeliveryHandler(Tracer tracer, Propagator propagator) {
            return new OutboxDeliveryTracingObservationHandler(tracer, propagator) {
                @Override
                protected Span.Builder customizeDeliverySpan(OutboxDeliveryContext context, Span.Builder builder) {
                    return builder.tag("test.own", "yes");
                }
            };
        }
    }

    /** No Boot auto-configuration but the outbox's: no registries or tracing, as an app without the actuator has. */
    @Configuration(proxyBeanMethods = false)
    static class Bare {

        @Bean
        DataSource dataSource() {
            return TestDataSources.newDataSource();
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    private static ApplicationContextRunner bare() {
        return new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
            .withUserConfiguration(Bare.class).withPropertyValues(properties());
    }

    private static ApplicationContextRunner traced(String... properties) {
        return runner(properties).withUserConfiguration(Metered.class, Traced.class);
    }

    @Test
    void metersNamedAsKora() {
        traced().withUserConfiguration(TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            var meters = context.getBean(MeterRegistry.class);

            send(context, "orders");

            await().atMost(WAIT).untilAsserted(() -> {
                assertThat(count(meters, "outbox.messages.delivered", "orders")).isOne();
                assertThat(timerCount(meters, "outbox.relay.step")).isPositive();
            });
            assertThat(count(meters, "outbox.messages.enqueued", "orders")).isOne();
            assertThat(meters.get("outbox.delivery.delay").tag("topic", "orders").timer().count()).isOne();
            assertThat(timerCount(meters, "outbox.enqueue")).isOne();
            assertThat(timerCount(meters, "outbox.deliver")).isOne();
        });
    }

    /**
     * Ours goes ahead of Boot's receiver handler in its first-matching group, Boot's stays for every other receiver;
     * the enqueue is a child of the caller's span, the delivery of the enqueue, linked to the step that delivered it.
     */
    @Test
    void deliverySpanLinksToTheStepSpan() {
        traced().withUserConfiguration(TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OutboxDeliveryTracingObservationHandler.class);
            assertThat(context.getBeansOfType(PropagatingReceiverTracingObservationHandler.class))
                .as("Boot's own receiver handler beside ours").hasSize(1);
            var spans = context.getBean(InMemorySpanExporter.class);

            Observation.createNotStarted("test.parent", context.getBean(ObservationRegistry.class))
                .observe(() -> send(context, "orders"));

            var delivery = awaitSpan(spans, "outbox deliver orders");
            var enqueue = span(spans, "outbox enqueue orders");
            var parent = span(spans, "test.parent");
            assertThat(enqueue.getParentSpanId()).isEqualTo(parent.getSpanId());
            assertThat(enqueue.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(delivery.getParentSpanId()).isEqualTo(enqueue.getSpanId());
            assertThat(delivery.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(delivery.getTraceId()).isEqualTo(parent.getTraceId());
            var link = assertThat(delivery.getLinks()).singleElement().actual().getSpanContext();
            // the step ends after its settle, so after the delivery
            await().atMost(WAIT).until(() -> spans.getFinishedSpanItems().stream()
                .anyMatch(s -> s.getSpanId().equals(link.getSpanId())));
            assertThat(spans.getFinishedSpanItems()).filteredOn(s -> s.getSpanId().equals(link.getSpanId()))
                .singleElement().satisfies(step -> {
                    assertThat(step.getName()).isEqualTo("outbox relay step");
                    assertThat(step.getKind()).isEqualTo(SpanKind.CLIENT);
                    assertThat(step.getTraceId()).isNotEqualTo(delivery.getTraceId());
                });
        });
    }

    /** Boot 3.5's actuator configures OpenTelemetry's Tracer and Propagator itself; Boot 4.1 needs a module more. */
    @Test
    @EnabledIfSystemProperty(named = "outbox.test.boot", matches = "3\\.5")
    void theDeliveryHandlerWithBootsOwnTracer() {
        runner().withUserConfiguration(TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(Traced.class).hasSingleBean(Tracer.class)
                .hasSingleBean(Propagator.class).hasSingleBean(OutboxDeliveryTracingObservationHandler.class);
            assertThat(context.getBeansOfType(PropagatingReceiverTracingObservationHandler.class)).hasSize(1);
        });
    }

    /** It replaces ours and takes the deliveries; Boot's receiver handler stays for every other receiver. */
    @Test
    void theAppsOwnDeliveryHandlerWins() {
        traced().withUserConfiguration(TestApps.WithRoute.class, OwnDeliveryHandler.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OutboxDeliveryTracingObservationHandler.class)
                .hasBean("ownDeliveryHandler");
            assertThat(context.getBeansOfType(PropagatingReceiverTracingObservationHandler.class).values())
                .as("Boot's own receiver handler").singleElement()
                .isNotInstanceOf(OutboxDeliveryTracingObservationHandler.class);
            var spans = context.getBean(InMemorySpanExporter.class);

            send(context, "orders");

            assertThat(awaitSpan(spans, "outbox deliver orders").getAttributes().get(AttributeKey.stringKey("test.own")))
                .isEqualTo("yes");
        });
    }

    @Test
    void handlerRunsInDeliverySpan() {
        traced().withBean(SpanSeeingRoute.class).run(context -> {
            assertThat(context).hasNotFailed();

            send(context, "orders");

            var delivery = awaitSpan(context.getBean(InMemorySpanExporter.class), "outbox deliver orders");
            var route = context.getBean(SpanSeeingRoute.class);
            assertThat(route.spanIds).containsExactly(delivery.getSpanId());
            assertThat(route.traceparents).as("the handed record carries the delivery span")
                .containsExactly("00-" + delivery.getTraceId() + "-" + delivery.getSpanId() + "-"
                    + delivery.getSpanContext().getTraceFlags().asHex());
        });
    }

    @Test
    void anUnsampledDeliveryIsNotObservedByDefault() {
        traced(UNSAMPLED).withUserConfiguration(TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            var meters = context.getBean(MeterRegistry.class);

            send(context, "orders");

            // the step's observation stops after its deliveries': the timer below would be there by then
            await().atMost(WAIT).untilAsserted(() -> {
                assertThat(count(meters, "outbox.messages.delivered", "orders")).isOne();
                assertThat(timerCount(meters, "outbox.relay.step")).isPositive();
            });
            assertThat(meters.find("outbox.deliver").timer()).isNull();
        });
    }

    /** The telemetry bean outlives the runtime: a stop and a start build a new one around it. */
    @Test
    void gaugesFollowTheNewRuntimeAfterARestart() {
        runner().withUserConfiguration(Metered.class, WithPoisonRoute.class)
            .withBean(TestOutboxConfig.class, () -> new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(100)))
            .run(context -> {
                assertThat(context).hasNotFailed();
                var meters = context.getBean(MeterRegistry.class);
                await().atMost(WAIT).until(() -> gauge(meters, SINCE_SUCCESS) < 1.0);

                context.stop();
                Thread.sleep(1500); // the closed runtime's last run is now older than the limit below
                context.start();

                await().atMost(WAIT).until(() -> gauge(meters, SINCE_SUCCESS) < 1.0);
                assertThat(gauge(meters, "outbox.dead.size")).isZero();
                send(context, "poison");
                await().atMost(WAIT).until(() -> gauge(meters, "outbox.dead.size") == 1);
                await().atMost(WAIT).until(() -> gauge(meters, SINCE_SUCCESS) < 1.0);
                assertThat(meters.find("outbox.dead.size").gauges()).hasSize(1);
            });
    }

    @Test
    void conventionBeanUsed() {
        traced().withUserConfiguration(TestApps.WithRoute.class, RenamingConventions.class).run(context -> {
            assertThat(context).hasNotFailed();
            var meters = context.getBean(MeterRegistry.class);

            send(context, "orders");

            await().atMost(WAIT).untilAsserted(() -> {
                assertThat(timerCount(meters, "custom.enqueue")).isOne();
                assertThat(timerCount(meters, "custom.deliver")).isOne();
                assertThat(timerCount(meters, "custom.step")).isPositive();
                assertThat(timerCount(meters, "custom.settle")).isPositive();
            });
            assertThat(meters.find("outbox.enqueue").timer()).isNull();
            assertThat(meters.find("outbox.deliver").timer()).isNull();
            assertThat(meters.find("outbox.relay.step").timer()).isNull();
            assertThat(meters.find("outbox.relay.settle").timer()).isNull();
        });
    }

    @Test
    void noRegistriesNoFailure() {
        bare().withUserConfiguration(TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed().hasBean("outboxTelemetry").doesNotHaveBean(MeterRegistry.class)
                .doesNotHaveBean(ObservationRegistry.class).doesNotHaveBean(Tracer.class)
                .doesNotHaveBean(OutboxDeliveryTracingObservationHandler.class);

            var id = send(context, "orders");

            var route = context.getBean(TestApps.OrdersRoute.class);
            await().atMost(WAIT).untilAsserted(() -> assertThat(route.delivered()).containsExactly(id));
        });
    }

    /** As the no-op Tracer Boot 4 adds without a bridge: no Propagator goes with it. */
    @Test
    void aTracerWithoutAPropagatorIsNoTracing() {
        bare().withUserConfiguration(TestApps.WithRoute.class).withBean(Tracer.class, () -> Tracer.NOOP)
            .run(context -> {
                assertThat(context).hasNotFailed().hasBean("outboxTelemetry")
                    .doesNotHaveBean(OutboxDeliveryTracingObservationHandler.class);

                var id = send(context, "orders");

                var route = context.getBean(TestApps.OrdersRoute.class);
                await().atMost(WAIT).untilAsserted(() -> assertThat(route.delivered()).containsExactly(id));
            });
    }

    /** As an app that registers its own tracing handlers: ours would trace each delivery a second time. */
    @Test
    void noDeliveryHandlerWithoutAReceiverHandler() {
        bare().withUserConfiguration(Traced.class, TestApps.WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(Tracer.class).hasSingleBean(Propagator.class)
                .doesNotHaveBean(PropagatingReceiverTracingObservationHandler.class)
                .doesNotHaveBean(OutboxDeliveryTracingObservationHandler.class);
        });
    }

    @Test
    void theAppsOwnTelemetryWins() {
        var own = new OutboxTelemetry() {};
        runner().withUserConfiguration(TestApps.WithRoute.class).withBean(OutboxTelemetry.class, () -> own)
            .run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(OutboxTelemetry.class);
                assertThat(context.getBean(OutboxTelemetry.class)).isSameAs(own);
            });
    }

    /**
     * On a real classpath without micrometer-tracing or its bridge. A FilteredClassLoader cannot show this: it hides
     * the classes from conditions, but the adapter's own classes, loaded above it, still link against them.
     */
    @Test
    @SuppressWarnings("unchecked")
    void withoutMicrometerTracingOnTheClasspath() throws Exception {
        var urls = Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(path -> !Path.of(path).getFileName().toString().contains("micrometer-tracing"))
            .map(TelemetryIT::url).toArray(URL[]::new);
        var thread = Thread.currentThread();
        var before = thread.getContextClassLoader();
        try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            thread.setContextClassLoader(loader);
            ((Consumer<String[]>) loader.loadClass(WithoutTracing.class.getName()).getDeclaredConstructor()
                .newInstance()).accept(properties());
        } finally {
            thread.setContextClassLoader(before);
        }
    }

    /** Runs inside that loader, so names no type of micrometer-tracing; {@code properties} name the Postgres. */
    public static final class WithoutTracing implements Consumer<String[]> {

        @Override
        public void accept(String[] properties) {
            assertThatThrownBy(() -> Class.forName("io.micrometer.tracing.Tracer"))
                .isInstanceOf(ClassNotFoundException.class);
            new ApplicationContextRunner()
                .withUserConfiguration(TestApps.Boot.class, Metered.class, TestApps.WithRoute.class)
                .withPropertyValues(properties).run(context -> {
                    assertThat(context).hasNotFailed().hasBean("outboxTelemetry");
                    var meters = context.getBean(MeterRegistry.class);

                    send(context, "orders");

                    await().atMost(WAIT).untilAsserted(() -> {
                        assertThat(count(meters, "outbox.messages.delivered", "orders")).isOne();
                        assertThat(timerCount(meters, "outbox.deliver")).isOne();
                    });
                });
        }
    }

    private static double count(MeterRegistry meters, String name, String topic) {
        var counter = meters.find(name).tag("topic", topic).counter();
        return counter == null ? 0 : counter.count();
    }

    private static double gauge(MeterRegistry meters, String name) {
        return meters.get(name).gauge().value();
    }

    /** 0 while the timer does not exist: {@code meters.get} would throw what {@code untilAsserted} does not retry. */
    private static long timerCount(MeterRegistry meters, String name) {
        var timer = meters.find(name).timer();
        return timer == null ? 0 : timer.count();
    }

    private static SpanData awaitSpan(InMemorySpanExporter spans, String name) {
        await().atMost(WAIT).until(() -> spans.getFinishedSpanItems().stream().anyMatch(s -> s.getName().equals(name)));
        return span(spans, name);
    }

    private static SpanData span(InMemorySpanExporter spans, String name) {
        return assertThat(spans.getFinishedSpanItems()).filteredOn(s -> s.getName().equals(name)).singleElement()
            .actual();
    }

    private static URL url(String path) {
        try {
            return Path.of(path).toUri().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }
}
