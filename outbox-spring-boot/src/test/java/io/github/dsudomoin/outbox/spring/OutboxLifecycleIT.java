package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.queryLong;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static io.github.dsudomoin.outbox.spring.TestApps.table;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.spring.TestApps.OrdersRoute;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** The outbox's start and stop within the application context. */
class OutboxLifecycleIT {

    private static final String NOT_STARTED = "the outbox starts with the application context's lifecycle; send"
        + " from an ApplicationRunner or later, not from @PostConstruct";
    private static final Duration WAIT = Duration.ofSeconds(10);

    /** What the runner sent. */
    static final class Sent {
        final List<UUID> ids = new CopyOnWriteArrayList<>();
    }

    @Configuration(proxyBeanMethods = false)
    @Import({TestApps.Boot.class, WithRoute.class})
    static class RunnerApp {

        @Bean
        Sent sent() {
            return new Sent();
        }

        @Bean
        ApplicationRunner sender(Outbox outbox, PlatformTransactionManager transactions, Sent sent) {
            return args -> sent.ids.add(new TransactionTemplate(transactions)
                .execute(status -> outbox.send(OutboxMessage.of("orders", "k", "v".getBytes(UTF_8)))));
        }
    }

    @Test
    void sendFromApplicationRunnerWorks() {
        try (var context = new SpringApplicationBuilder(RunnerApp.class).web(WebApplicationType.NONE)
                .properties(TestApps.properties()).run()) {
            var sent = context.getBean(Sent.class).ids;
            assertThat(sent).hasSize(1);

            await().atMost(WAIT).until(() -> context.getBean(OrdersRoute.class).delivered().containsAll(sent));
        }
    }

    static final class EarlySender {

        private final Outbox outbox;

        EarlySender(Outbox outbox) {
            this.outbox = outbox;
        }

        @PostConstruct
        void send() {
            outbox.send(OutboxMessage.of("orders", "k", "v".getBytes(UTF_8)));
        }
    }

    @Test
    void sendFromPostConstructFailsWithMessage() {
        runner().withUserConfiguration(WithRoute.class).withBean(EarlySender.class).run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().isInstanceOf(IllegalStateException.class)
                .hasMessage(NOT_STARTED));
    }

    /** Records its start and stop, and whether the outbox ran at the time. */
    static final class Recorder implements SmartLifecycle {

        private final int phase;
        private final BeanFactory beans; // not the lifecycle itself: a dependency would order the stops
        private final List<String> events;
        private volatile boolean running;

        Recorder(int phase, BeanFactory beans, List<String> events) {
            this.phase = phase;
            this.beans = beans;
            this.events = events;
        }

        @Override
        public void start() {
            events.add("start " + phase + " outbox running " + beans.getBean(OutboxLifecycle.class).isRunning());
            running = true;
        }

        @Override
        public void stop() {
            events.add("stop " + phase + " outbox running " + beans.getBean(OutboxLifecycle.class).isRunning());
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int getPhase() {
            return phase;
        }
    }

    static final class Events {
        final List<String> list = new CopyOnWriteArrayList<>();
    }

    /** The web server's phase, the Kafka listener containers', the default one. */
    @Configuration(proxyBeanMethods = false)
    static class Recorders {

        @Bean
        Events events() {
            return new Events();
        }

        @Bean
        Recorder webServer(BeanFactory beans, Events events) {
            return new Recorder(Integer.MAX_VALUE - 2048, beans, events.list);
        }

        @Bean
        Recorder listenerContainers(BeanFactory beans, Events events) {
            return new Recorder(Integer.MAX_VALUE - 100, beans, events.list);
        }

        @Bean
        Recorder scheduler(BeanFactory beans, Events events) {
            return new Recorder(Integer.MAX_VALUE, beans, events.list);
        }
    }

    @Test
    void startAndStopOrder() {
        runner().withUserConfiguration(WithRoute.class, Recorders.class).run(context -> {
            assertThat(context).hasNotFailed();
            var instances = "select count(*) from " + table(context) + "_instance";
            assertThat(queryLong(instances)).isOne();
            var events = context.getBean(Events.class).list;

            context.close();

            assertThat(events).containsExactly(
                "start " + (Integer.MAX_VALUE - 2048) + " outbox running true",
                "start " + (Integer.MAX_VALUE - 100) + " outbox running true",
                "start " + Integer.MAX_VALUE + " outbox running true",
                "stop " + Integer.MAX_VALUE + " outbox running true",
                "stop " + (Integer.MAX_VALUE - 100) + " outbox running true",
                "stop " + (Integer.MAX_VALUE - 2048) + " outbox running true");
            assertThat(queryLong(instances)).as("heartbeat row deleted at stop").isZero();
        });
    }

    /**
     * Fails the refresh after the lifecycle beans started; Spring then destroys the beans without stopping them, as it
     * does only for a failed start.
     */
    static final class FailingListener implements ApplicationListener<ContextRefreshedEvent> {

        @Override
        public void onApplicationEvent(ContextRefreshedEvent event) {
            throw new IllegalStateException("listener fails");
        }
    }

    @Test
    void failedRefreshStopsTheOutbox() {
        var table = TestSql.uniqueTable();
        runner("outbox.table=" + table).withUserConfiguration(WithRoute.class).withBean(FailingListener.class)
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(queryLong("select count(*) from " + table + "_instance")).as("heartbeat row").isZero();
            });
    }

    /** Blocks in its first call until released. */
    static final class BlockingRoute implements OutboxTopicHandler {

        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final List<UUID> handled = new CopyOnWriteArrayList<>();

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            entered.countDown();
            release.await();
            handled.add(record.messageId());
        }
    }

    @Test
    void gracefulShutdownWithBatchInFlight() {
        var table = new String[1];
        var sent = new UUID[1];
        runner().withBean(BlockingRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            table[0] = table(context);
            var route = context.getBean(BlockingRoute.class);
            var jdbc = new JdbcTemplate(context.getBean(DataSource.class));
            sent[0] = send(context, "orders");
            assertThat(route.entered.await(10, TimeUnit.SECONDS)).isTrue();

            var closer = new Thread(context::close);
            closer.start();
            closer.join(300);
            assertThat(closer.isAlive()).as("close waits for the batch in flight").isTrue();
            assertThat(jdbc.queryForObject("select 1", Integer.class)).as("the DataSource still open").isOne();
            route.release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(closer.isAlive()).as("closed within the shutdown timeout").isFalse();
            assertThat(route.handled).containsExactly(sent[0]);
            assertThat(queryLong("select count(*) from " + table[0] + "_instance")).as("heartbeat row").isZero();
        });

        runner("outbox.table=" + table[0]).withUserConfiguration(WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            var later = send(context, "orders");

            await().atMost(WAIT).until(() -> context.getBean(OrdersRoute.class).delivered().contains(later));
            assertThat(context.getBean(OrdersRoute.class).delivered()).as("the outcome was recorded")
                .doesNotContain(sent[0]);
        });
    }

    @Test
    void restartAfterStop() {
        runner().withUserConfiguration(WithRoute.class).run(context -> {
            var lifecycle = context.getBean(OutboxLifecycle.class);
            var first = lifecycle.runtime();

            context.stop();
            assertThat(lifecycle.isRunning()).isFalse();
            context.start();

            assertThat(lifecycle.isRunning()).isTrue();
            assertThat(lifecycle.runtime()).isNotSameAs(first);
            var id = send(context, "orders");
            await().atMost(WAIT).until(() -> context.getBean(OrdersRoute.class).delivered().contains(id));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class WithFallback {

        @Bean
        RecordingHandler fallback() {
            return new RecordingHandler();
        }
    }

    @Test
    void sendAfterStopIsDurable() {
        runner().withUserConfiguration(WithFallback.class).run(context -> {
            var handler = context.getBean(RecordingHandler.class);
            context.stop();

            var id = send(context, "unrouted");

            assertThat(queryLong("select count(*) from " + table(context) + " where message_id = '" + id + "'"))
                .isOne();
            Thread.sleep(200);
            assertThat(handler.delivered()).isEmpty();
            context.start();
            await().atMost(WAIT).until(() -> handler.delivered().stream().anyMatch(r -> r.messageId().equals(id)));
        });
    }
}
