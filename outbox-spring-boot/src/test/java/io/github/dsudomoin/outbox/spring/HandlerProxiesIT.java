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
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.aopalliance.intercept.MethodInterceptor;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.junit.jupiter.api.Test;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
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

        @Override
        protected void handleTombstone(OutboxRecord record) {
            calls.add("tombstone");
        }
    }

    /** Overrides {@code handleBatch}, which then is what the relay calls on the proxy. */
    static class TypedBatch extends Typed {

        TypedBatch(Calls calls) {
            super(calls);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    static class PlainBatch implements OutboxTopicHandler {

        private final Calls calls;

        PlainBatch(Calls calls) {
            this.calls = calls;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) {
            calls.add("plain");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return OutboxTopicHandler.super.handleBatch(records);
        }
    }

    static class FallbackBatch implements OutboxHandler {

        private final Calls calls;

        FallbackBatch(Calls calls) {
            this.calls = calls;
        }

        @Override
        public void handle(OutboxRecord record) {
            calls.add("fallback");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return OutboxHandler.super.handleBatch(records);
        }
    }

    /** The relay enters through its handleBatch: handle runs only through the override's own calls, past the proxy. */
    static class PlainTxBatch extends PlainTx {

        PlainTxBatch(Calls calls) {
            super(calls);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    /** The same for its hook. */
    static class TypedTxBatch extends TypedTx {

        TypedTxBatch(Calls calls) {
            super(calls);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Counted {}

    @Counted
    static class CountedTypedBatch extends TypedBatch {

        CountedTypedBatch(Calls calls) {
            super(calls);
        }
    }

    @Counted
    static class CountedTyped extends Typed {

        CountedTyped(Calls calls) {
            super(calls);
        }
    }

    interface AuditApi {
        void handle(AuditEvent event, OutboxRecord record) throws Exception;
    }

    /** Its hook is an interface method too, but the wrapper of a JDK proxy still calls only handle(OutboxRecord). */
    static class TypedWithApi extends TypedOutboxTopicHandler<AuditEvent> implements AuditApi {

        TypedWithApi() {
            super(Set.of("audit"), HandlerProxiesIT::decode);
        }

        @Override
        @Transactional(rollbackFor = Exception.class)
        public void handle(AuditEvent event, OutboxRecord record) {}
    }

    interface TxOnInterface extends OutboxTopicHandler {

        @Override
        @Transactional(rollbackFor = Exception.class)
        void handle(OutboxRecord record) throws Exception;
    }

    /** Its {@code handle(OutboxRecord)} is the final one, which a class-based proxy cannot intercept. */
    static class TypedTxOnInterface extends TypedOutboxTopicHandler<AuditEvent> implements TxOnInterface {

        TypedTxOnInterface() {
            super(Set.of("audit"), HandlerProxiesIT::decode);
        }

        @Override
        protected void handle(AuditEvent event, OutboxRecord record) {}
    }

    static class UnrelatedBase {

        public void handle(OutboxRecord record) {}
    }

    /** A class-based proxy calls the base's {@code handle}, in which Spring finds no annotation of the interface. */
    static class ViaBaseTxOnInterface extends UnrelatedBase implements TxOnInterface {

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }
    }

    /** Proxied for {@code other}; its final {@code handle} would run on the proxy's own instance. */
    static class FinalHandle implements OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public final void handle(OutboxRecord record) {}

        @Transactional
        public void other() {}
    }

    /** Proxied for {@code other}; its final hook would run on the proxy's own instance. */
    static class FinalHook extends TypedOutboxTopicHandler<AuditEvent> {

        FinalHook() {
            super(Set.of("audit"), HandlerProxiesIT::decode);
        }

        @Override
        protected final void handle(AuditEvent event, OutboxRecord record) {}

        @Transactional
        public void other() {}
    }

    /** Proxied for {@code other}; its final {@code handleBatch} would run on the proxy's own instance. */
    static class FinalBatch implements OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) {}

        @Override
        public final List<Outcome> handleBatch(List<OutboxRecord> records) {
            return List.of();
        }

        @Transactional
        public void other() {}
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

    /** Records each call of {@code handleBatch} of the bean {@code batched}. */
    @Aspect
    static class BatchAspect {

        private final Calls calls;

        BatchAspect(Calls calls) {
            this.calls = calls;
        }

        @Before("execution(* handleBatch(java.util.List)) && bean(batched)")
        void record() {
            calls.add("batch aspect");
        }
    }

    /** Records each call of {@code handleTombstone} of the bean {@code typedHandler}. */
    @Aspect
    static class TombstoneAspect {

        private final Calls calls;

        TombstoneAspect(Calls calls) {
            this.calls = calls;
        }

        @Before("execution(* handleTombstone(..)) && bean(typedHandler)")
        void record() {
            calls.add("tombstone aspect");
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

    @Configuration(proxyBeanMethods = false)
    static class CountedTypedApp {

        /** Class-wide: its method matcher takes every method, the final one and the default handleBatch too. */
        @Bean
        static Advisor countingAdvisor(ObjectProvider<Calls> calls) {
            return new DefaultPointcutAdvisor(new AnnotationMatchingPointcut(Counted.class, true),
                (MethodInterceptor) invocation -> {
                    calls.getObject().add("advice");
                    return invocation.proceed();
                });
        }

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new CountedTyped(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CountedTypedBatchApp {

        @Bean
        static Advisor countingAdvisor(ObjectProvider<Calls> calls) {
            return CountedTypedApp.countingAdvisor(calls);
        }

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new CountedTypedBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PlainTxBatchApp {

        @Bean
        OutboxTopicHandler txBatch(Calls calls) {
            return new PlainTxBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedTxBatchApp {

        @Bean
        OutboxTopicHandler txBatch(Calls calls) {
            return new TypedTxBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedWithApiApp {

        @Bean
        OutboxTopicHandler typedHandler() {
            return new TypedWithApi();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithBatchAspect {

        @Bean
        BatchAspect batchAspect(Calls calls) {
            return new BatchAspect(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WithBatchAspect.class)
    static class PlainBatchApp {

        @Bean
        OutboxTopicHandler batched(Calls calls) {
            return new PlainBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WithBatchAspect.class)
    static class FallbackBatchApp {

        @Bean
        OutboxHandler batched(Calls calls) {
            return new FallbackBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WithBatchAspect.class)
    static class TypedBatchApp {

        @Bean
        OutboxTopicHandler batched(Calls calls) {
            return new TypedBatch(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WithBatchAspect.class)
    static class PlainDefaultBatchApp {

        @Bean
        OutboxTopicHandler batched(Calls calls) {
            return new PlainTx(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(WithBatchAspect.class)
    static class TypedDefaultBatchApp {

        @Bean
        OutboxTopicHandler batched(Calls calls) {
            return new Typed(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TombstoneAspectApp {

        @Bean
        OutboxTopicHandler typedHandler(Calls calls) {
            return new Typed(calls);
        }

        @Bean
        TombstoneAspect tombstoneAspect(Calls calls) {
            return new TombstoneAspect(calls);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedTxOnInterfaceApp {

        @Bean
        OutboxTopicHandler typedHandler() {
            return new TypedTxOnInterface();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ViaBaseTxOnInterfaceApp {

        @Bean
        OutboxTopicHandler ordersRoute() {
            return new ViaBaseTxOnInterface();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FinalHandleApp {

        @Bean
        OutboxTopicHandler finalBean() {
            return new FinalHandle();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FinalHookApp {

        @Bean
        OutboxTopicHandler finalBean() {
            return new FinalHook();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FinalBatchApp {

        @Bean
        OutboxTopicHandler finalBean() {
            return new FinalBatch();
        }
    }

    /** An interface-based proxy over a class-based one: its final handle(OutboxRecord) runs on the inner proxy. */
    @Configuration(proxyBeanMethods = false)
    static class NestedProxiesApp {

        @Bean
        OutboxTopicHandler nested(Calls calls) {
            var inner = new ProxyFactory(new Typed(calls));
            inner.setProxyTargetClass(true);
            return (OutboxTopicHandler) new ProxyFactory(inner.getProxy()).getProxy();
        }
    }

    /** A prototype has no name to report; the other bean claims its topic. */
    @Configuration(proxyBeanMethods = false)
    static class PrototypeApp {

        @Bean
        @Scope("prototype")
        OutboxTopicHandler prototypeRoute(Calls calls) {
            return new PlainTx(calls);
        }

        @Bean
        OutboxTopicHandler singletonRoute(Calls calls) {
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

    private static List<String> seenAfterOne(ApplicationContext context, String topic) {
        send(context, topic);
        var seen = context.getBean(Calls.class).seen;
        await().atMost(WAIT).until(() -> seen.size() >= 2);
        return seen;
    }

    private static List<String> seenAfterTombstone(ApplicationContext context) {
        var outbox = context.getBean(Outbox.class);
        new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
            .executeWithoutResult(status -> outbox.send(OutboxMessage.of("audit", "k", (byte[]) null)));
        var seen = context.getBean(Calls.class).seen;
        await().atMost(WAIT).until(() -> seen.size() >= 2);
        return seen;
    }

    static String failure(AssertableApplicationContext context) {
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
    void transactionalOnInterfaceFailsStartOnClassProxy() {
        app(TypedTxOnInterfaceApp.class).run(context -> assertThat(failure(context)).contains("typedHandler")
            .contains("handle(OutboxRecord)").contains("advise the hook"));
        app(ViaBaseTxOnInterfaceApp.class).run(context -> assertThat(failure(context)).contains("ordersRoute")
            .contains("handle(OutboxRecord)").contains("only through an interface"));
    }

    @Test
    void scopedProxyHandlerFailsStart() {
        app(ScopedApp.class).run(context ->
            assertThat(failure(context)).contains("ordersRoute").contains("proxy without a singleton target"));
    }

    @Test
    void classWideAdvisorOnTypedHandlerRunsPerRecord() {
        var perRecord = new String[] {"advice no tx", "typed v no tx", "advice no tx", "typed v no tx"};
        app(CountedTypedApp.class).run(context ->
            assertThat(seenAfter(context, "audit", 4)).containsExactly(perRecord));
        app(CountedTypedApp.class, JDK).run(context ->
            assertThat(seenAfter(context, "audit", 4)).containsExactly(perRecord));
    }

    @Test
    void jdkTypedHookDeclaredByAnInterfaceStillFailsStart() {
        app(TypedWithApiApp.class, JDK).run(context -> assertThat(failure(context)).contains("typedHandler")
            .contains("handle(" + AuditEvent.class.getSimpleName() + ", OutboxRecord)")
            .contains("spring.aop.proxy-target-class=true"));
    }

    @Test
    void aspectOnInheritedHandleBatchFailsStart() {
        for (var run : List.of(app(PlainDefaultBatchApp.class), app(PlainDefaultBatchApp.class, JDK),
                app(TypedDefaultBatchApp.class), app(TypedDefaultBatchApp.class, JDK))) {
            run.run(context -> assertThat(failure(context)).contains("batched").contains("handleBatch(List)")
                .contains(BatchAspect.class.getName()).contains("override handleBatch or advise handle"));
        }
    }

    @Test
    void finalCalledMethodOnClassBasedProxyFailsStart() {
        app(FinalHandleApp.class).run(context -> assertThat(failure(context)).contains("finalBean")
            .contains("handle(OutboxRecord)").contains("is final"));
        app(FinalHookApp.class).run(context -> assertThat(failure(context)).contains("finalBean")
            .contains("handle(" + AuditEvent.class.getSimpleName() + ", OutboxRecord)").contains("is final"));
        app(FinalBatchApp.class).run(context -> assertThat(failure(context)).contains("finalBean")
            .contains("handleBatch(List)").contains("is final"));
        app(NestedProxiesApp.class).run(context -> assertThat(failure(context)).contains("nested")
            .contains("handle(OutboxRecord)").contains("is final"));
    }

    @Test
    void aspectOnOverriddenHandleBatchRuns() {
        for (var properties : List.of(new String[0], new String[] {JDK})) {
            app(PlainBatchApp.class, properties).run(context -> assertThat(seenAfterOne(context, "orders"))
                .containsExactly("batch aspect no tx", "plain no tx"));
            app(FallbackBatchApp.class, properties).run(context -> assertThat(seenAfterOne(context, "other"))
                .containsExactly("batch aspect no tx", "fallback no tx"));
            app(TypedBatchApp.class, properties).run(context -> assertThat(seenAfterOne(context, "audit"))
                .containsExactly("batch aspect no tx", "typed v no tx"));
        }
    }

    @Test
    void adviceOnHandleOfBatchOverrideFailsStart() {
        for (var properties : List.of(new String[0], new String[] {JDK})) {
            app(PlainTxBatchApp.class, properties).run(context -> assertThat(failure(context)).contains("txBatch")
                .contains("handle(OutboxRecord)").contains("TransactionAttributeSourceAdvisor")
                .contains("advise handleBatch, or use a TransactionTemplate inside the override"));
            app(TypedTxBatchApp.class, properties).run(context -> assertThat(failure(context)).contains("txBatch")
                .contains("handle(" + AuditEvent.class.getSimpleName() + ", OutboxRecord)")
                .contains("TransactionAttributeSourceAdvisor")
                .contains("advise handleBatch, or use a TransactionTemplate inside the override"));
        }
    }

    @Test
    void classWideAdvisorOnBatchOverrideRuns() {
        for (var properties : List.of(new String[0], new String[] {JDK})) {
            app(CountedTypedBatchApp.class, properties).run(context -> assertThat(seenAfterOne(context, "audit"))
                .containsExactly("advice no tx", "typed v no tx"));
        }
    }

    @Test
    void tombstoneReachesTheAdvice() {
        app(TombstoneAspectApp.class).run(context -> assertThat(seenAfterTombstone(context))
            .containsExactly("tombstone aspect no tx", "tombstone no tx"));
        app(TypedAspectApp.class, JDK).run(context -> assertThat(seenAfterTombstone(context))
            .containsExactly("aspect no tx", "tombstone no tx"));
    }

    @Test
    void unnamedBeanBehindJdkProxyNamedByItsClass() {
        app(PrototypeApp.class, JDK).run(context -> assertThat(failure(context))
            .contains("is claimed by both beans ").contains(PlainTx.class.getName() + " (" + PlainTx.class.getName())
            .doesNotContain("$Proxy"));
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
