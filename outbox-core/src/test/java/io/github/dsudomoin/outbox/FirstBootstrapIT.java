package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** The first bootstrap of a table makes ranges for what is running then, not only for what starts after it. */
class FirstBootstrapIT {

    private static final DataSource DIRECT = TestDataSources.newDataSource();

    @Test
    void aTransactionOlderThanTheFirstBootstrapFindsItsRange() throws Exception {
        var transactions = new JdbcTransactions(DIRECT);
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).relayEnabled(false);
        var handler = new RecordingHandler();
        try (var runtime = OutboxRuntime.builder(config, transactions, handler).build()) {
            var id = transactions.inTx(c -> {
                TestSql.execute(c, "select pg_current_xact_id()"); // it holds an xid several ranges below the next one
                burnXids(3500);
                start(runtime);
                return runtime.outbox().send(OutboxMessage.of("t", "k", "p".getBytes(UTF_8)));
            });

            runtime.relay().drain();

            assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
            try (var c = DIRECT.getConnection()) {
                assertThat(TestSql.queryLong(c, "select count(*) from pg_inherits where inhparent = to_regclass('"
                        + runtime.schema().names().base() + "')"))
                        .as("the gap is one range, not one per xidsPerPartition").isLessThanOrEqualTo(4 + 3);
            }
        }
    }

    private static void start(OutboxRuntime runtime) throws SQLException {
        try {
            CompletableFuture.runAsync(runtime::start).get(); // as another instance would, on its own connections
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private static void burnXids(int n) throws SQLException {
        try (var c = DIRECT.getConnection(); var st = c.createStatement()) {
            for (int i = 0; i < n; i++) {
                st.execute("select pg_current_xact_id()");
            }
        }
    }
}
