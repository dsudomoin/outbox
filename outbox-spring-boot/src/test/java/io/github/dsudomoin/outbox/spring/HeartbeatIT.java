package io.github.dsudomoin.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * The heartbeat's connections: a pool of one connection derived from the app's Hikari pool by default, so beats go on
 * while business load holds every app connection; the app's connections with {@code outbox.heartbeat
 * .dedicated-connection=false} or when the pool cannot be derived; an {@link OutboxHeartbeat} DataSource whatever the
 * key says. The derived pool closes after the runtime has deleted its row. Mirrors the Kora adapter's HeartbeatPoolIT.
 */
class HeartbeatIT {

    private static final Duration INTERVAL = Duration.ofSeconds(1);
    /** 3 intervals: older than that, the row has expired. */
    private static final long STALE_MILLIS = 3 * INTERVAL.toMillis();
    private static final Duration WAIT = Duration.ofSeconds(10);

    /** The app pool's name, so the derived one's is known. */
    private final String pool = "app" + TestSql.uniqueTable();

    /** Boot's Hikari pool of two connections, a one-second heartbeat, no relay. */
    private ApplicationContextRunner runner(String... more) {
        return TestApps.runner(Stream.concat(Stream.of("outbox.partitions.maintenance-interval=1s",
                "outbox.relay.enabled=false", "spring.datasource.hikari.maximum-pool-size=2",
                "spring.datasource.hikari.pool-name=" + pool), Stream.of(more)).toArray(String[]::new))
            .withUserConfiguration(WithRoute.class);
    }

    @Test
    void beatsContinueWhileTheAppPoolIsHeld() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(poolRuns()).as("the derived pool").isTrue();
            var run = hold(context);

            assertThat(run.second()).as("beats while the app pool is held").isGreaterThan(run.first());
            assertThat(run.age()).isLessThan(STALE_MILLIS);
        });
    }

    @Test
    void withTheKeyOffBeatsStall() {
        try (var log = new CapturedStderr()) {
            runner("outbox.heartbeat.dedicated-connection=false").run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(poolRuns()).as("a pool derived from the app's").isFalse();
                var run = hold(context);

                assertThat(run.age()).isGreaterThan(STALE_MILLIS);
            });
            assertThat(log.text()).containsOnlyOnce("outbox heartbeat shares the application's connections")
                .containsOnlyOnce("in Spring Boot: outbox.heartbeat.dedicated-connection");
        }
    }

    /** Counts the connections it gives. */
    static class CountingDataSource extends DelegatingDataSource {

        final AtomicInteger taken = new AtomicInteger();

        CountingDataSource(DataSource target) {
            super(target);
        }

        @Override
        public Connection getConnection() throws SQLException {
            taken.incrementAndGet();
            return super.getConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithHeartbeatDataSource {

        @Bean(defaultCandidate = false)
        @OutboxHeartbeat
        CountingDataSource heartbeatDataSource() {
            return new CountingDataSource(TestDataSources.newDataSource());
        }
    }

    @Test
    void aQualifiedDataSourceWinsOverTheKeyOff() {
        qualifiedDataSourceServesTheBeats(false);
    }

    @Test
    void aQualifiedDataSourceWinsOverTheKeyOn() {
        qualifiedDataSourceServesTheBeats(true);
    }

    /** Beats on the qualified DataSource, with no pool derived whatever the key says. */
    private void qualifiedDataSourceServesTheBeats(boolean dedicated) {
        runner("outbox.heartbeat.dedicated-connection=" + dedicated)
            .withUserConfiguration(WithHeartbeatDataSource.class).run(context -> {
                assertThat(context).hasNotFailed();
                var taken = context.getBean("heartbeatDataSource", CountingDataSource.class).taken;
                var names = names(context);
                int before = taken.get();
                awaitBeat(names);
                awaitBeat(names);

                assertThat(taken.get()).as("connections the qualified DataSource gave across two beats")
                    .isGreaterThan(before);
                assertThat(poolRuns()).as("a pool derived from the app's").isFalse();
            });
    }

    /** Claims to wrap a HikariDataSource that is not started and has no URL: a pool derived from it fails to build. */
    static final class BrokenHikariWrapper extends CountingDataSource {

        private final HikariDataSource unbuildable = new HikariDataSource();

        BrokenHikariWrapper() {
            super(TestDataSources.newDataSource());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return iface == HikariDataSource.class || super.isWrapperFor(iface);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return iface == HikariDataSource.class ? iface.cast(unbuildable) : super.unwrap(iface);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithBrokenHikari {

        @Bean
        BrokenHikariWrapper dataSource() {
            return new BrokenHikariWrapper();
        }
    }

    @Test
    void aFailedDerivationSharesTheAppConnections() {
        try (var log = new CapturedStderr()) {
            runner().withUserConfiguration(WithBrokenHikari.class).run(context -> {
                assertThat(context).hasNotFailed();
                var taken = context.getBean(BrokenHikariWrapper.class).taken;
                var names = names(context);
                int before = taken.get();
                awaitBeat(names);
                awaitBeat(names);

                assertThat(taken.get()).as("app connections across two beats").isGreaterThan(before);
            });
            assertThat(log.text()).containsOnlyOnce("could not derive a pool of its own")
                .containsOnlyOnce("outbox heartbeat shares the application's connections");
        }
    }

    /** A Hikari pool behind a wrapper, which unwraps to it; closed with the context. */
    static final class WrappedHikari extends DelegatingDataSource implements Closeable {

        WrappedHikari(HikariDataSource target) {
            super(target);
        }

        @Override
        public void close() {
            ((HikariDataSource) obtainTargetDataSource()).close();
        }
    }

    @Test
    void aWrapperOfAHikariPoolIsCopied() {
        runner().withBean(DataSource.class, () -> {
            var hikari = new HikariDataSource();
            hikari.setJdbcUrl(TestDataSources.jdbcUrl());
            hikari.setUsername(TestDataSources.user());
            hikari.setPassword(TestDataSources.password());
            hikari.setMaximumPoolSize(2);
            hikari.setPoolName(pool);
            return new WrappedHikari(hikari);
        }).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(poolRuns()).as("the pool derived from the wrapped one").isTrue();
            var run = hold(context);

            assertThat(run.age()).isLessThan(STALE_MILLIS);
        });
        await().atMost(WAIT).until(() -> !poolRuns());
    }

    @Test
    void contextCloseDeletesTheRowBeforeClosingThePool() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            var names = names(context);
            assertThat(poolRuns()).as("the derived pool, before the close").isTrue();
            try (var log = new CapturedStderr()) {
                context.close();

                assertThat(log.text()).doesNotContain("this beat uses the application's connections")
                    .doesNotContain("could not delete its row");
            }
            assertThat(TestApps.queryLong("select count(*) from " + names.instance())).isZero();
            await().atMost(WAIT).until(() -> !poolRuns());
        });
    }

    /** The build that checks the handlers fails in the bean method, before the start that derives the pool. */
    @Test
    void aFailedBuildDerivesNoPool() {
        try (var log = new CapturedStderr()) {
            TestApps.runner("spring.datasource.hikari.pool-name=" + pool).run(context ->
                assertThat(context).as("no handler, no route").hasFailed().getFailure().rootCause()
                    .isInstanceOf(IllegalArgumentException.class));
            assertThat(log.text()).doesNotContain(derived() + " - Starting...");
        }
    }

    @Test
    void aFailedStartClosesThePool() {
        try (var log = new CapturedStderr()) {
            runner("outbox.table=s" + TestSql.uniqueTable() + ".outbox") // in a schema that does not exist
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("does not exist");
                });
            assertThat(log.text()).as("derived for the start").containsOnlyOnce(derived() + " - Starting...");
        }
        await().atMost(WAIT).until(() -> !poolRuns());
    }

    /** A refresh that fails before the start, after the lifecycle's creation, derives no pool. */
    @Test
    void aRefreshThatFailsBeforeTheStartDerivesNoPool() {
        try (var log = new CapturedStderr()) {
            runner().withBean(OutboxLifecycleIT.EarlySender.class).run(context -> assertThat(context).hasFailed());
            assertThat(log.text()).doesNotContain(derived() + " - Starting...");
        }
    }

    @Test
    void withoutHikariNoDeriverLoads() {
        try (var log = new CapturedStderr()) {
            runner().withClassLoader(new FilteredClassLoader(HikariDataSource.class))
                .withBean(DataSource.class, TestDataSources::newDataSource).run(context -> {
                    assertThat(context).hasNotFailed()
                        .doesNotHaveBean(OutboxAutoConfiguration.HeartbeatPools.class);

                    assertThat(log.text())
                        .containsOnlyOnce("give it one as a DataSource bean qualified @OutboxHeartbeat");
                });
        }
    }

    @Test
    void withNothingToCopyOneInfoNamesTheQualifier() {
        try (var log = new CapturedStderr()) {
            runner().withBean(DataSource.class, TestDataSources::newDataSource).run(context -> {
                assertThat(context).hasNotFailed();

                assertThat(log.text()).as("PGSimpleDataSource is no HikariDataSource")
                    .containsOnlyOnce("give it one as a DataSource bean qualified @OutboxHeartbeat");
                assertThat(threadRuns("outbox-heartbeat:housekeeper")).as("a pool derived from nothing").isFalse();
            });
        }
    }

    /** Boot binds the app pool's meters after every singleton is instantiated; the pool is derived after that. */
    @Test
    void theDerivedPoolHasTheAppPoolsMetersFromTheFirstStart() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            var registry = context.getBean(MeterRegistry.class);

            assertThat(registry.find("hikaricp.connections").tag("pool", pool).gauge()).as("the app pool's")
                .isNotNull();
            assertThat(registry.find("hikaricp.connections").tag("pool", derived()).gauge()).as("the derived pool's")
                .isNotNull();
            assertThat(registry.find("hikaricp.connections.max").tag("pool", derived()).gauge().value()).isOne();
        });
    }

    @Test
    void aStartAfterAStopDerivesANewPool() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(poolRuns()).isTrue();

            context.stop();
            await().atMost(WAIT).until(() -> !poolRuns());
            context.start();

            assertThat(poolRuns()).as("a new derived pool").isTrue();
            var run = hold(context);
            assertThat(run.age()).isLessThan(STALE_MILLIS);
        });
    }

    /** {@code last_seen} sampled twice, 2 s apart, while both app connections are held, and its age as they go back. */
    private record Run(long first, long second, long age) {}

    /** Waits for a beat, then holds both app connections for 5 s, measuring. */
    private static Run hold(ApplicationContext context) throws Exception {
        var names = names(context);
        awaitBeat(names);
        var app = context.getBean(DataSource.class);
        try (var a = app.getConnection(); var b = app.getConnection()) {
            Thread.sleep(1000);
            long first = millis("last_seen", names);
            Thread.sleep(2000);
            long second = millis("last_seen", names);
            Thread.sleep(2000);
            return new Run(first, second, millis("now() - last_seen", names));
        }
    }

    private static TableNames names(ApplicationContext context) {
        return context.getBean(OutboxLifecycle.class).runtime().schema().names();
    }

    /** The name of the pool derived from the app's. */
    private String derived() {
        return pool + "-outbox-heartbeat";
    }

    /** Whether the pool derived from the app's runs: Hikari names its housekeeper thread after the pool. */
    private boolean poolRuns() {
        return threadRuns(derived() + ":housekeeper");
    }

    private static boolean threadRuns(String name) {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name));
    }

    /** Waits until {@code last_seen} moves past its value now. */
    private static void awaitBeat(TableNames names) throws SQLException {
        long seen = millis("last_seen", names);
        await().atMost(WAIT).until(() -> millis("last_seen", names) > seen);
    }

    /** {@code expression} of the instance's row, in milliseconds since the epoch or as a duration. */
    private static long millis(String expression, TableNames names) throws SQLException {
        return TestApps.queryLong("select (extract(epoch from " + expression + ") * 1000)::bigint from "
            + names.instance());
    }
}
