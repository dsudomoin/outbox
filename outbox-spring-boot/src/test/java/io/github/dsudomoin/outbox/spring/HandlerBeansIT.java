package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.failure;
import static io.github.dsudomoin.outbox.spring.TestApps.rootCause;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

/** How the handler beans become the runtime's handler and routes, and the start failures that name them. */
class HandlerBeansIT {

    static final class Fallback implements OutboxHandler {
        @Override
        public void handle(OutboxRecord record) {}
    }

    static final class Other implements OutboxHandler {
        @Override
        public void handle(OutboxRecord record) {}
    }

    /** A route of the topics it is given. */
    static final class Route implements OutboxTopicHandler {

        private final Set<String> topics;

        Route(String... topics) {
            this.topics = Set.of(topics);
        }

        @Override
        public Set<String> topics() {
            return topics;
        }

        @Override
        public void handle(OutboxRecord record) {}
    }

    static final class Both implements OutboxHandler, OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("both");
        }

        @Override
        public void handle(OutboxRecord record) {}

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return OutboxHandler.super.handleBatch(records);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoHandlers {

        @Bean
        OutboxHandler first() {
            return new Fallback();
        }

        @Bean
        @Primary
        OutboxHandler second() {
            return new Other();
        }
    }

    @Test
    void severalOutboxHandlersFail() {
        runner().withUserConfiguration(TwoHandlers.class).run(context -> assertThat(failure(context))
            .contains("several OutboxHandler beans: ").contains("first (" + Fallback.class.getName() + ")")
            .contains("second (" + Other.class.getName() + ")").doesNotContain("outbox.kafka.fallback"));
    }

    @Test
    void severalOutboxHandlersNameTheKafkaKey() {
        runner().withBean("outboxKafkaFallbackHandler", Fallback.class).withBean("appHandler", Other.class)
            .run(context -> assertThat(failure(context))
                .contains("several OutboxHandler beans: ").contains("outboxKafkaFallbackHandler").contains("appHandler")
                .endsWith("; outbox-kafka-spring-boot provides outboxKafkaFallbackHandler while"
                    + " outbox.kafka.fallback=true: set outbox.kafka.fallback=false to let your own handler"
                    + " take them"));
    }

    @Configuration(proxyBeanMethods = false)
    static class KafkaInParent {

        @Bean
        OutboxHandler outboxKafkaFallbackHandler() {
            return new Fallback();
        }
    }

    @Test
    void parentContextHandlerIsNamedToo() {
        try (var parent = new AnnotationConfigApplicationContext(KafkaInParent.class)) {
            runner().withParent(parent).withBean("appHandler", Other.class).run(context ->
                assertThat(failure(context)).contains("outboxKafkaFallbackHandler (" + Fallback.class.getName() + ")")
                    .contains("appHandler").contains("outbox.kafka.fallback=false"));
        }
    }

    @Test
    void bothRolesBesideKafkaKeepTheirOwnMessage() {
        runner().withBean("both", Both.class).withUserConfiguration(KafkaInParent.class)
            .run(context -> assertThat(failure(context))
                .contains("both (" + Both.class.getName() + ")").contains("implement OutboxTopicHandler only")
                .doesNotContain("several OutboxHandler beans"));
    }

    @Configuration(proxyBeanMethods = false)
    static class BothRolesByInterface {

        @Bean
        @Lazy
        OutboxHandler both() {
            return new Both();
        }
    }

    @Test
    void bothRolesDeclaredByInterfaceBesideKafkaKeepTheirOwnMessage() {
        runner().withUserConfiguration(BothRolesByInterface.class, KafkaInParent.class).run(context ->
            assertThat(failure(context)).contains("implement OutboxTopicHandler only")
                .doesNotContain("several OutboxHandler beans"));
    }

    @Configuration(proxyBeanMethods = false)
    static class NullHandlerBesideKafka {

        @Bean
        OutboxHandler maybe() {
            return null;
        }
    }

    @Test
    void aHandlerFactoryReturningNullIsNoConflict() {
        runner().withUserConfiguration(NullHandlerBesideKafka.class, KafkaInParent.class).run(context ->
            assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    static class UnbuildableKafkaBesideAppHandler {

        @Bean
        @Lazy // as the Kafka module's is, so its failure reaches the handler lookup
        OutboxHandler outboxKafkaFallbackHandler() {
            throw new IllegalArgumentException("bootstrap.servers is required");
        }

        @Bean
        OutboxHandler appHandler() {
            return new Other();
        }
    }

    @Test
    void aFallbackThatCannotBuildStillNamesTheConflict() {
        runner().withUserConfiguration(UnbuildableKafkaBesideAppHandler.class).run(context -> {
            assertThat(failure(context)).contains("several OutboxHandler beans: ")
                .contains("outboxKafkaFallbackHandler").contains("appHandler").contains("outbox.kafka.fallback=false");
            assertThat(rootCause(context).getSuppressed()).isNotEmpty();
        });
    }

    @Test
    void duplicateTopicNamesBeans() {
        runner().withBean("auditOne", OutboxTopicHandler.class, () -> new Route("audit"))
            .withBean("auditTwo", OutboxTopicHandler.class, () -> new Route("billing", "audit"))
            .run(context -> assertThat(failure(context))
                .contains("outbox topic audit is claimed by both beans ").contains("auditOne").contains("auditTwo"));
    }

    @Test
    void emptyTopicsNamesBean() {
        runner().withBean("emptyRoute", OutboxTopicHandler.class, () -> new Route())
            .run(context -> assertThat(failure(context)).contains("emptyRoute").contains("claims no topics"));
    }

    @Test
    void startupLineListsRoutes() {
        try (var stderr = new CapturedStderr()) {
            runner().withUserConfiguration(WithRoute.class).withBean("appFallback", Fallback.class)
                .withBean("auditRoute", OutboxTopicHandler.class, () -> new Route("billing", "audit"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(stderr.text()).containsOnlyOnce("outbox routes: ")
                        .contains("outbox routes: audit -> auditRoute, billing -> auditRoute, orders -> ordersRoute;"
                            + " other topics -> appFallback");
                });
        }
        try (var stderr = new CapturedStderr()) {
            runner().withUserConfiguration(WithRoute.class).run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(stderr.text())
                    .contains("outbox routes: orders -> ordersRoute; other topics -> refused (no OutboxHandler)");
            });
        }
    }
}
