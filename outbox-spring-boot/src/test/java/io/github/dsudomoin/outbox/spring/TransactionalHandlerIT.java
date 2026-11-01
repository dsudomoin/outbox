package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.HandlerProxiesIT.failure;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.OutboxWaitException;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.RollbackOn;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Handler transactions with the Inbox: a handler that ends with an error leaves no marker, no effect and no follow-up
 * event behind, and a handler whose transaction could commit on an error fails the start.
 */
class TransactionalHandlerIT {

    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final String ROLLBACK_FIX = "use @Transactional(rollbackFor = Exception.class) without"
        + " noRollbackFor, or @EnableTransactionManagement(rollbackOn = RollbackOn.ALL_EXCEPTIONS)";
    private static final String BATCH_FIX = "put @Transactional(rollbackFor = Exception.class) on handle and keep the"
        + " default handleBatch, or use a TransactionTemplate per record inside handleBatch, or one around the batch"
        + " that throws on any failure";
    /** No transaction proxies, as under AspectJ weaving: Boot's auto-configuration by either line's name. */
    private static final String NO_TX_PROXIES = "spring.autoconfigure.exclude="
        + "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration,"
        + "org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration";

    /** What an attempt does after its marker and its effect, in their transaction; the first attempt is 1. */
    interface Step {
        void run(int attempt) throws Exception;
    }

    /** The work of every handler here: the marker, the effect when the marker is new, then the attempt's step. */
    static final class Effects {

        private final JdbcTemplate jdbc;
        private final Inbox inbox;
        private final String table;
        final AtomicInteger attempts = new AtomicInteger();
        final List<Boolean> accepted = new CopyOnWriteArrayList<>();
        /** The effects each attempt found, read before it writes: committed ones, and its own transaction's. */
        final List<Long> rowsBefore = new CopyOnWriteArrayList<>();
        volatile Step step = attempt -> {};

        Effects(JdbcTemplate jdbc, Inbox inbox, String table) {
            this.jdbc = jdbc;
            this.inbox = inbox;
            this.table = table;
        }

        long rows() {
            return jdbc.queryForObject("select count(*) from " + table, Long.class);
        }

        void apply(OutboxRecord record) throws Exception {
            int attempt = attempts.incrementAndGet();
            rowsBefore.add(rows());
            boolean isNew = inbox.tryAccept(record.messageId());
            accepted.add(isNew);
            if (isNew) {
                jdbc.update("insert into " + table + " (message_id) values (?)", record.messageId());
            }
            step.run(attempt);
        }

        /** For a {@link TransactionTemplate} callback, which takes no checked exception. */
        void applyUnchecked(OutboxRecord record) {
            try {
                apply(record);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static class RollbackForHandler implements OutboxTopicHandler {

        private final Effects effects;

        RollbackForHandler(Effects effects) {
            this.effects = effects;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        @Transactional(rollbackFor = Exception.class)
        public void handle(OutboxRecord record) throws Exception {
            effects.apply(record);
        }
    }

    static class PlainHandler implements OutboxTopicHandler {

        private final Effects effects;

        PlainHandler(Effects effects) {
            this.effects = effects;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        @Transactional
        public void handle(OutboxRecord record) throws Exception {
            effects.apply(record);
        }
    }

    static class EffectService {

        private final Effects effects;

        EffectService(Effects effects) {
            this.effects = effects;
        }

        @Transactional(rollbackFor = Exception.class)
        public void apply(OutboxRecord record) throws Exception {
            effects.apply(record);
        }
    }

    static class ViaService implements OutboxTopicHandler {

        private final EffectService service;

        ViaService(EffectService service) {
            this.service = service;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            service.apply(record);
        }
    }

    /** A transaction per record; {@code Delivered} only for those it committed. */
    static class TemplatePerRecord implements OutboxTopicHandler {

        private final Effects effects;
        private final TransactionTemplate transaction;

        TemplatePerRecord(Effects effects, TransactionTemplate transaction) {
            this.effects = effects;
            this.transaction = transaction;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) {
            throw new AssertionError("the override of handleBatch does not call it");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var outcomes = new ArrayList<Outcome>();
            for (var record : records) { // distinct keys here: no failed key to skip
                try {
                    transaction.executeWithoutResult(status -> effects.applyUnchecked(record));
                    outcomes.add(Outcome.delivered());
                } catch (RuntimeException e) {
                    outcomes.add(Outcome.retry(e));
                }
            }
            return outcomes;
        }
    }

    /** One transaction for the batch, thrown out of on any failure. */
    static class TemplateAroundBatch implements OutboxTopicHandler {

        private final Effects effects;
        private final TransactionTemplate transaction;
        final List<Integer> calls = new CopyOnWriteArrayList<>();

        TemplateAroundBatch(Effects effects, TransactionTemplate transaction) {
            this.effects = effects;
            this.transaction = transaction;
        }

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) {
            throw new AssertionError("the override of handleBatch does not call it");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            calls.add(records.size());
            transaction.executeWithoutResult(status -> records.forEach(effects::applyUnchecked));
            return records.stream().map(record -> Outcome.delivered()).toList();
        }
    }

    static class FollowUps implements OutboxTopicHandler {

        final List<UUID> delivered = new CopyOnWriteArrayList<>();

        @Override
        public Set<String> topics() {
            return Set.of("follow-ups");
        }

        @Override
        public void handle(OutboxRecord record) {
            delivered.add(record.messageId());
        }
    }

    /** Handlers that only have to start or fail to. */
    abstract static class Orders implements OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }
    }

    static class PlainHandle extends Orders {

        @Override
        @Transactional
        public void handle(OutboxRecord record) {}
    }

    @Transactional
    static class ClassLevelPlain extends Orders {

        @Override
        public void handle(OutboxRecord record) {}
    }

    static class NoRollbackFor extends Orders {

        @Override
        @Transactional(rollbackFor = Exception.class, noRollbackFor = IOException.class)
        public void handle(OutboxRecord record) {}
    }

    static class RequiresNewPlain extends Orders {

        @Override
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        public void handle(OutboxRecord record) {}
    }

    static class NoTransactionPropagations extends Orders {

        @Override
        @Transactional(propagation = Propagation.NOT_SUPPORTED)
        public void handle(OutboxRecord record) {}

        @Override
        @Transactional(propagation = Propagation.SUPPORTS)
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    static class TransactionalBatch extends Orders {

        @Override
        public void handle(OutboxRecord record) {}

        @Override
        @Transactional(rollbackFor = Exception.class)
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    /** Would be told about its rollback policy first if the override did not matter more. */
    @Transactional
    static class ClassLevelPlainBatch extends Orders {

        @Override
        public void handle(OutboxRecord record) {}

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return super.handleBatch(records);
        }
    }

    static class PlainFallback implements OutboxHandler {

        @Override
        @Transactional
        public void handle(OutboxRecord record) {}
    }

    record Audit(String text) {}

    abstract static class TypedOrders extends TypedOutboxTopicHandler<Audit> {

        TypedOrders() {
            super(Set.of("audit"), (topic, payload) -> new Audit(new String(payload, UTF_8)));
        }
    }

    static class PlainHook extends TypedOrders {

        @Override
        @Transactional
        protected void handle(Audit event, OutboxRecord record) {}
    }

    @Transactional
    static class ClassLevelPlainTyped extends TypedOrders {

        @Override
        protected void handle(Audit event, OutboxRecord record) {}
    }

    static class PlainTombstone extends TypedOrders {

        @Override
        @Transactional(rollbackFor = Exception.class)
        protected void handle(Audit event, OutboxRecord record) {}

        @Override
        @Transactional
        protected void handleTombstone(OutboxRecord record) {}
    }

    static class WovenRollbackFor extends Orders {

        @Override
        @Transactional(rollbackFor = Exception.class)
        public void handle(OutboxRecord record) {}
    }

    @Configuration(proxyBeanMethods = false)
    static class Common {

        @Bean
        Effects effects(JdbcTemplate jdbc, Inbox inbox, @Value("${business.table}") String table) {
            return new Effects(jdbc, inbox, table);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RollbackForApp {

        @Bean
        OutboxTopicHandler handler(Effects effects) {
            return new RollbackForHandler(effects);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(rollbackOn = RollbackOn.ALL_EXCEPTIONS)
    static class AllExceptionsApp {

        @Bean
        OutboxTopicHandler handler(Effects effects) {
            return new PlainHandler(effects);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ServiceApp {

        @Bean
        EffectService effectService(Effects effects) {
            return new EffectService(effects);
        }

        @Bean
        OutboxTopicHandler handler(EffectService service) {
            return new ViaService(service);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TemplatePerRecordApp {

        @Bean
        OutboxTopicHandler handler(Effects effects, PlatformTransactionManager transactions) {
            return new TemplatePerRecord(effects, new TransactionTemplate(transactions));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TemplateAroundBatchApp {

        @Bean
        TemplateAroundBatch handler(Effects effects, PlatformTransactionManager transactions) {
            return new TemplateAroundBatch(effects, new TransactionTemplate(transactions));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FollowUpApp {

        @Bean
        FollowUps followUps() {
            return new FollowUps();
        }
    }

    /** The source AspectJ's transaction aspect reads, with no transaction proxy in the context. */
    @Configuration(proxyBeanMethods = false)
    static class WovenTransactions {

        @Bean
        static TransactionAttributeSource transactionAttributeSource() {
            return new AnnotationTransactionAttributeSource(false);
        }
    }

    /** Proxies the bean {@code aspected} for something other than transactions. */
    @Aspect
    static class OtherAspect {

        @Before("execution(* handle(..)) && bean(aspected)")
        void before() {}
    }

    private static ApplicationContextRunner app(Class<?> config, String... properties) throws SQLException {
        var table = TestSql.uniqueTable();
        TestApps.execute("create table " + table + " (message_id uuid primary key)");
        return runner(properties).withPropertyValues("business.table=" + table)
            .withUserConfiguration(Common.class, config);
    }

    /** A bean whose start is the whole test; no business table, no Inbox. */
    private static <T> ApplicationContextRunner starting(String name, Class<T> type, T bean, String... properties) {
        return runner(properties).withBean(name, type, () -> bean);
    }

    private static Effects effects(ApplicationContext context) {
        return context.getBean(Effects.class);
    }

    /** True when {@code id} has no committed marker; records none. */
    private static boolean noMarker(ApplicationContext context, UUID id) {
        var inbox = context.getBean(Inbox.class);
        return new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).execute(status -> {
            status.setRollbackOnly();
            return inbox.tryAccept(id);
        });
    }

    /** Three records in one transaction. */
    private static List<UUID> sendThree(ApplicationContext context, String... keys) {
        var outbox = context.getBean(Outbox.class);
        return new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).execute(status -> List.of(
            outbox.send(OutboxMessage.of("orders", keys[0], "v".getBytes(UTF_8))),
            outbox.send(OutboxMessage.of("orders", keys[1], "v".getBytes(UTF_8))),
            outbox.send(OutboxMessage.of("orders", keys[2], "v".getBytes(UTF_8)))));
    }

    /** Waits until no record is left in the retry, parked and waiting tables. */
    private static void awaitSettled(ApplicationContext context) {
        var admin = context.getBean(OutboxAdmin.class);
        await().atMost(WAIT).until(() -> {
            var status = admin.status();
            return status.retrySize() == 0 && status.parkedSize() == 0 && status.waitingSize() == 0;
        });
    }

    @Test
    void checkedExceptionRollsBackEverything() throws Exception {
        app(RollbackForApp.class).run(context -> assertRetriedFromNothing(context));
    }

    private static void assertRetriedFromNothing(ApplicationContext context) {
        var effects = effects(context);
        effects.step = attempt -> {
            if (attempt == 1) {
                throw new IOException("first");
            }
        };
        send(context, "orders");
        await().atMost(WAIT).until(() -> effects.attempts.get() >= 2 && effects.rows() == 1);
        awaitSettled(context);
        assertThat(effects.rowsBefore).containsExactly(0L, 0L);
        assertThat(effects.accepted).containsExactly(true, true);
        assertThat(effects.rows()).isEqualTo(1);
    }

    @Test
    void permanentAndWaitRollBack() throws Exception {
        app(RollbackForApp.class).run(context -> {
            var effects = effects(context);
            effects.step = attempt -> {
                throw new OutboxPermanentException("never");
            };
            var id = send(context, "orders");
            var admin = context.getBean(OutboxAdmin.class);
            await().atMost(WAIT).until(() -> admin.countDeadLetters(DeadLetterFilter.any()) == 1);
            assertThat(effects.rows()).isZero();
            assertThat(noMarker(context, id)).isTrue();
        });
        app(RollbackForApp.class).run(context -> {
            var effects = effects(context);
            effects.step = attempt -> {
                throw new OutboxWaitException("not now");
            };
            var id = send(context, "orders");
            var admin = context.getBean(OutboxAdmin.class);
            await().atMost(WAIT).until(() -> admin.status().waitingSize() == 1 && effects.attempts.get() >= 2);
            assertThat(effects.rows()).isZero();
            assertThat(noMarker(context, id)).isTrue();
            effects.step = attempt -> {};
            await().atMost(WAIT).until(() -> effects.rows() == 1);
        });
    }

    @Test
    void reHandAfterCommitIsNoOp() throws Exception {
        app(RollbackForApp.class, "outbox.relay.batch-timeout=2s").run(context -> {
            var effects = effects(context);
            effects.step = attempt -> {
                if (attempt == 1) {
                    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            holdUntilHandedAgain(effects);
                        }
                    });
                }
            };
            send(context, "orders");
            await().atMost(WAIT).until(() -> effects.attempts.get() >= 2 && effects.accepted.size() >= 2);
            awaitSettled(context);
            assertThat(effects.accepted).containsExactly(true, false);
            assertThat(effects.attempts).hasValue(2);
            assertThat(effects.rows()).isEqualTo(1);
        });
    }

    /** Ignores the interrupts of the relay's wind-down and cancellation: the commit has already happened. */
    private static void holdUntilHandedAgain(Effects effects) {
        long until = System.nanoTime() + WAIT.toNanos();
        while (effects.attempts.get() < 2 && System.nanoTime() < until) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // keep holding
            }
        }
    }

    @Test
    void plainTransactionalHandleRefused() {
        starting("refused", OutboxTopicHandler.class, new PlainHandle()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)").contains(ROLLBACK_FIX));
    }

    @Test
    void plainTransactionalHandleRefusedOnInterfaceProxy() {
        starting("refused", OutboxTopicHandler.class, new PlainHandle(), "spring.aop.proxy-target-class=false")
            .run(context -> assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)")
                .contains(ROLLBACK_FIX));
    }

    @Test
    void plainTransactionalFallbackRefused() {
        starting("refused", OutboxHandler.class, new PlainFallback()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)").contains(ROLLBACK_FIX));
    }

    @Test
    void plainTransactionalTypedHookRefused() {
        starting("refused", OutboxTopicHandler.class, new PlainHook()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(Audit, OutboxRecord)")
                .contains(ROLLBACK_FIX));
    }

    @Test
    void plainTransactionalTombstoneRefused() {
        starting("refused", OutboxTopicHandler.class, new PlainTombstone()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handleTombstone(OutboxRecord)")
                .contains(ROLLBACK_FIX));
    }

    @Test
    void classLevelPlainTransactionalRefused() {
        starting("refused", OutboxTopicHandler.class, new ClassLevelPlain()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)").contains(ROLLBACK_FIX));
        starting("refused", OutboxTopicHandler.class, new ClassLevelPlainTyped()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(Audit, OutboxRecord)")
                .contains(ROLLBACK_FIX));
    }

    @Test
    void noRollbackForRefused() {
        starting("refused", OutboxTopicHandler.class, new NoRollbackFor()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)").contains(ROLLBACK_FIX));
    }

    @Test
    void transactionalHandleBatchOverrideRefused() {
        starting("refused", OutboxTopicHandler.class, new TransactionalBatch()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handleBatch(List)").contains(BATCH_FIX));
        starting("refused", OutboxTopicHandler.class, new ClassLevelPlainBatch()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handleBatch(List)").contains(BATCH_FIX));
    }

    @Test
    void requiresNewPlainRefused() {
        starting("refused", OutboxTopicHandler.class, new RequiresNewPlain()).run(context ->
            assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)").contains(ROLLBACK_FIX));
    }

    @Test
    void propagationsWithoutATransactionStart() {
        starting("started", OutboxTopicHandler.class, new NoTransactionPropagations()).run(context ->
            assertThat(context).hasNotFailed());
    }

    @Test
    void unproxiedBeanCheckedThroughTheContextSource() {
        starting("refused", OutboxTopicHandler.class, new PlainHandle(), NO_TX_PROXIES)
            .withUserConfiguration(WovenTransactions.class)
            .run(context -> assertThat(failure(context)).contains("refused").contains("handle(OutboxRecord)")
                .contains(ROLLBACK_FIX));
        starting("started", OutboxTopicHandler.class, new WovenRollbackFor(), NO_TX_PROXIES)
            .withUserConfiguration(WovenTransactions.class)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(AopUtils.isAopProxy(context.getBean("started"))).isFalse();
            });
    }

    @Test
    void beanProxiedForOtherAdviceCheckedThroughTheContextSource() {
        starting("aspected", OutboxTopicHandler.class, new PlainHandle(), NO_TX_PROXIES)
            .withUserConfiguration(WovenTransactions.class)
            .withBean(OtherAspect.class, OtherAspect::new)
            .run(context -> assertThat(failure(context)).contains("aspected").contains("handle(OutboxRecord)")
                .contains(ROLLBACK_FIX));
    }

    @Test
    void allExceptionsPolicyStarts() throws Exception {
        app(AllExceptionsApp.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertRetriedFromNothing(context);
        });
    }

    @Test
    void templatePerRecordInBatch() throws Exception {
        app(TemplatePerRecordApp.class).run(context -> {
            var effects = effects(context);
            effects.step = attempt -> {
                if (attempt == 2) {
                    throw new IOException("second record of the first call");
                }
            };
            sendThree(context, "a", "b", "c");
            await().atMost(WAIT).until(() -> effects.rows() == 3);
            awaitSettled(context);
            assertThat(effects.attempts).hasValue(4);
            assertThat(effects.accepted).containsExactly(true, true, true, true);
        });
    }

    @Test
    void templateAroundBatchThatThrows() throws Exception {
        app(TemplateAroundBatchApp.class).run(context -> {
            var effects = effects(context);
            effects.step = attempt -> {
                if (attempt == 2) {
                    throw new IOException("second record of the first call");
                }
            };
            sendThree(context, "k", "k", "k"); // one key: one shard, one call
            await().atMost(WAIT).until(() -> effects.rows() == 3);
            awaitSettled(context);
            assertThat(context.getBean(TemplateAroundBatch.class).calls.get(0)).isEqualTo(3);
            assertThat(effects.rowsBefore.get(2)).isZero(); // the third attempt opens the next call: nothing committed
            assertThat(effects.accepted).containsOnly(true).hasSize(effects.attempts.get());
        });
    }

    @Test
    void followUpSendJoinsHandlerTransaction() throws Exception {
        app(RollbackForApp.class).withUserConfiguration(FollowUpApp.class).run(context -> {
            var effects = effects(context);
            var outbox = context.getBean(Outbox.class);
            var sent = new CopyOnWriteArrayList<UUID>();
            effects.step = attempt -> {
                sent.add(outbox.send(OutboxMessage.of("follow-ups", "k", "v".getBytes(UTF_8))));
                if (attempt == 1) {
                    throw new IOException("first");
                }
            };
            send(context, "orders");
            var followUps = context.getBean(FollowUps.class);
            await().atMost(WAIT).until(() -> effects.rows() == 1 && !followUps.delivered.isEmpty());
            awaitSettled(context);
            assertThat(sent).hasSize(2);
            assertThat(followUps.delivered).containsExactly(sent.get(1));
        });
    }

    @Test
    void serviceShapeFromDocs() throws Exception {
        app(ServiceApp.class).run(context -> assertRetriedFromNothing(context));
    }
}
