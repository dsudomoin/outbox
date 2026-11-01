package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static io.github.dsudomoin.outbox.spring.TestApps.table;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.context.support.SimpleThreadScope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Handler beans behind Spring AOP proxies: their advice runs per record, or the start fails saying why it cannot. */
class HandlerProxiesIT {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String JDK = "spring.aop.proxy-target-class=false";

    /** What the handlers and the aspect saw, in order. */
    static final class Calls {

        final List<String> seen = new CopyOnWriteArrayList<>();

        void add(String what) {
            seen.add(what + (TransactionSynchronizationManager.isActualTransactionActive() ? " in tx" : " no tx"));
        }
    }

    @OutboxContract(2)
    record AuditEvent(String text) {}

    private static AuditEvent decode(String topic, byte[] payload) {
        return new AuditEvent(new String(payload, UTF_8));
    }

    static class PlainTx implements OutboxTopicHandler {

        private final Calls calls;

        PlainTx(Calls calls) {
            this.calls = calls;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        @Transactional(rollbackFor = Exception.class)
        public void handle(OutboxRecord record) {
            calls.add("plain");
        }
    }

    static class TypedTx extends TypedOutboxTopicHandler<AuditEvent> {

        private final Calls calls;

        TypedTx(Calls calls) {
            super(Set.of("audit"), HandlerProxiesIT::decode);
            this.calls = calls;
        }

        @Override
        @Transactional(rollbackFor = Exception.class)
        protected void handle(AuditEvent event, OutboxRecord record) {
            calls.add("typed " + event.text());
        }
    }

    @Transactional(rollbackFor = Exception.class)
    static class ClassTxTyped extends TypedOutboxTopicHandler<AuditEvent> {

        private final Calls calls;

        ClassTxTyped(Calls calls) {
            super(Set.of("audit"), HandlerProxiesIT::decode);
            this.calls = calls;
        }

        @Override
        protected void handle(AuditEvent event, OutboxRecord record) {
            calls.add("typed " + event.text());
        }
    }

    static class FallbackTx implements OutboxHandler {

        private final Calls calls;

        FallbackTx(Calls calls) {
            this.calls = calls;
        }

        @Override
        @Transactional(rollbackFor = Exception.class)
        public void handle(OutboxRecord record) {
            calls.add("fallback");
        }
    }

    static class Typed extends TypedOutboxTopicHandler<AuditEvent> {

        private final Calls calls;

        Typed(Calls calls) {
            super(Set.of("audit"), HandlerProxiesIT::decode);
            this.calls = calls;
        }

        @Override
        protected void handle(AuditEvent event, OutboxRecord record) {
            calls.add("typed " + event.text());
        }
    }

    /** Records each call of the bean {@code typedHandler}'s interface {@code handle(OutboxRecord)}. */
    @Aspect
    static class RecordingAspect {

        private final Calls calls;

        RecordingAspect(Calls calls) {
            this.calls = calls;
        }

        @Before("execution(void io.github.dsudomoin.outbox.OutboxTopicHandler.handle("
            + "io.github.dsudomoin.outbox.OutboxRecord)) && bean(typedHandler)")
        void record() {
            calls.add("aspect");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Common {

        @Bean
        Calls calls() {
            return new Calls();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PlainTxApp {

        @Bean
        OutboxTopicHandler ordersRoute(Calls calls) {
            return new PlainTx(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedTxApp {

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new TypedTx(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ClassTxTypedApp {

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new ClassTxTyped(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FallbackTxApp {

        @Bean
        OutboxHandler fallback(Calls calls) {
            return new FallbackTx(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedAspectApp {

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new Typed(calls);
        }

        @Bean
        RecordingAspect recordingAspect(Calls calls) {
            return new RecordingAspect(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ScopedApp {

        @Bean
        static CustomScopeConfigurer threadScope() {
            var configurer = new CustomScopeConfigurer();
            configurer.addScope("thread", new SimpleThreadScope());
            return configurer;
        }

        @Bean
        @Scope(scopeName = "thread", proxyMode = ScopedProxyMode.TARGET_CLASS)
        OutboxTopicHandler ordersRoute(Calls calls) {
            return new PlainTx(calls);
        }
    }

    private static ApplicationContextRunner app(Class<?> config, String... properties) {
        return runner(properties).withUserConfiguration(Common.class, config);
    }

    private static List<String> seenAfter(ApplicationContext context, String topic, int calls) {
        send(context, topic);
        send(context, topic);
        var seen = context.getBean(Calls.class).seen;
        await().atMost(WAIT).until(() -> seen.size() >= calls);
        return seen;
    }

    private static String failure(AssertableApplicationContext context) {
        assertThat(context).hasFailed();
        var cause = context.getStartupFailure();
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    @Test
    void plainTransactionalHandlePerRecord() {
        app(PlainTxApp.class).run(context ->
            assertThat(seenAfter(context, "orders", 2)).containsExactly("plain in tx", "plain in tx"));
    }

    @Test
    void typedTransactionalHookPerRecord() {
        app(TypedTxApp.class).run(context ->
            assertThat(seenAfter(context, "audit", 2)).containsExactly("typed v in tx", "typed v in tx"));
    }

    @Test
    void classLevelTransactionalTyped() {
        app(ClassTxTypedApp.class).run(context ->
            assertThat(seenAfter(context, "audit", 2)).containsExactly("typed v in tx", "typed v in tx"));
    }

    @Test
    void fallbackOutboxHandlerProxied() {
        app(FallbackTxApp.class).run(context ->
            assertThat(seenAfter(context, "other", 2)).containsExactly("fallback in tx", "fallback in tx"));
    }

    @Test
    void aspectOnFinalTypedHandleFailsStart() {
        app(TypedAspectApp.class).run(context -> assertThat(failure(context)).contains("typedHandler")
            .contains("handle(OutboxRecord)").contains(RecordingAspect.class.getName()).contains("advise the hook"));
    }

    @Test
    void jdkPlainTransactionalWorks() {
        app(PlainTxApp.class, JDK).run(context ->
            assertThat(seenAfter(context, "orders", 2)).containsExactly("plain in tx", "plain in tx"));
    }

    @Test
    void jdkTypedAspectOnInterfaceHandleRuns() {
        app(TypedAspectApp.class, JDK).run(context -> assertThat(seenAfter(context, "audit", 4))
            .containsExactly("aspect no tx", "typed v no tx", "aspect no tx", "typed v no tx"));
    }

    @Test
    void jdkTypedTransactionalHookFailsStart() {
        app(TypedTxApp.class, JDK).run(context -> assertThat(failure(context)).contains("typedHandler")
            .contains("handle(" + AuditEvent.class.getSimpleName() + ", OutboxRecord)")
            .contains("spring.aop.proxy-target-class=true"));
    }

    @Test
    void scopedProxyHandlerFailsStart() {
        app(ScopedApp.class).run(context ->
            assertThat(failure(context)).contains("ordersRoute").contains("proxy without a singleton target"));
    }

    /** A v2 send is checked against the reports, and this instance reports what the proxied handler's target reads. */
    @Test
    void typedContractVersionFromTarget() {
        app(TypedTxApp.class).run(context -> assertContractV2(context, "typed v2 in tx"));
        app(TypedAspectApp.class, JDK).run(context -> assertContractV2(context, "typed v2 no tx"));
    }

    private static void assertContractV2(ApplicationContext context, String delivered) throws Exception {
        assertThat(context.getBean(OutboxLifecycle.class).isRunning()).isTrue();
        assertThat(new JdbcTemplate(context.getBean(DataSource.class)).queryForObject(
            "select contracts->>'audit' from " + table(context) + "_instance", String.class)).isEqualTo("2");
        var typed = new TypedOutbox<>(context.getBean(Outbox.class),
            (topic, event) -> event.text().getBytes(UTF_8), AuditEvent.class);

        new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
            .executeWithoutResult(status -> typed.send("audit", "k", new AuditEvent("v2")));

        var seen = context.getBean(Calls.class).seen;
        await().atMost(WAIT).until(() -> seen.contains(delivered));
    }
}
