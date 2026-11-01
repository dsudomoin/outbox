package io.github.dsudomoin.outbox.spring;

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
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    private static String failure(AssertableApplicationContext context) {
        assertThat(context).hasFailed();
        var cause = context.getStartupFailure();
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
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

    @Configuration(proxyBeanMethods = false)
    static class AppHandlerBesideKafka {

        @Bean
        OutboxHandler outboxKafkaFallbackHandler() {
            return new Fallback();
        }

        @Bean
        OutboxHandler appHandler() {
            return new Other();
        }
    }

    @Test
    void severalOutboxHandlersNameTheKafkaKey() {
        runner().withUserConfiguration(AppHandlerBesideKafka.class).run(context -> assertThat(failure(context))
            .contains("several OutboxHandler beans: ").contains("outboxKafkaFallbackHandler").contains("appHandler")
            .endsWith("; outbox-kafka-spring-boot provides outboxKafkaFallbackHandler while"
                + " outbox.kafka.fallback=true: set outbox.kafka.fallback=false to let your own handler take them"));
    }

    @Configuration(proxyBeanMethods = false)
    static class KafkaInParent {

        @Bean
        OutboxHandler outboxKafkaFallbackHandler() {
            return new Fallback();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AppHandlerOnly {

        @Bean
        OutboxHandler appHandler() {
            return new Other();
        }
    }

    @Test
    void parentContextHandlerIsNamedToo() {
        try (var parent = new AnnotationConfigApplicationContext(KafkaInParent.class)) {
            runner().withParent(parent).withUserConfiguration(AppHandlerOnly.class).run(context ->
                assertThat(failure(context)).contains("outboxKafkaFallbackHandler (" + Fallback.class.getName() + ")")
                    .contains("appHandler").contains("outbox.kafka.fallback=false"));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BothRoles {

        @Bean
        Both both() {
            return new Both();
        }
    }

    @Test
    void bothRolesRefused() {
        runner().withUserConfiguration(BothRoles.class).run(context -> assertThat(failure(context))
            .contains("both (" + Both.class.getName() + ")").contains("implement OutboxTopicHandler only"));
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoAuditRoutes {

        @Bean
        OutboxTopicHandler auditOne() {
            return new Route("audit");
        }

        @Bean
        OutboxTopicHandler auditTwo() {
            return new Route("billing", "audit");
        }
    }

    @Test
    void duplicateTopicNamesBeans() {
        runner().withUserConfiguration(TwoAuditRoutes.class).run(context -> assertThat(failure(context))
            .contains("outbox topic audit is claimed by both beans ").contains("auditOne").contains("auditTwo"));
    }

    @Configuration(proxyBeanMethods = false)
    static class EmptyRoute {

        @Bean
        OutboxTopicHandler emptyRoute() {
            return new Route();
        }
    }

    @Test
    void emptyTopicsNamesBean() {
        runner().withUserConfiguration(EmptyRoute.class).run(context -> assertThat(failure(context))
            .contains("emptyRoute").contains("claims no topics"));
    }

    @Configuration(proxyBeanMethods = false)
    static class WithFallback {

        @Bean
        OutboxHandler appFallback() {
            return new Fallback();
        }

        @Bean
        OutboxTopicHandler auditRoute() {
            return new Route("billing", "audit");
        }
    }

    @Test
    void startupLineListsRoutes() {
        try (var stderr = new CapturedStderr()) {
            runner().withUserConfiguration(WithRoute.class, WithFallback.class).run(context -> {
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
