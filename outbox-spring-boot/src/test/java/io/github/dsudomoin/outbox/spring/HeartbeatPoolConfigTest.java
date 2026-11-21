package io.github.dsudomoin.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

/** The heartbeat pool's config, copied from plain Hikari configs: no pool, no database. */
class HeartbeatPoolConfigTest {

    @Test
    void deriveSkipsWhatItCannotCopy() throws Exception {
        var interval = Duration.ofSeconds(30);

        assertThat(HeartbeatPool.derive(new JdbcTransactions(new PGSimpleDataSource()), interval))
            .as("not SpringOutboxTransactions").isNull();
        assertThat(HeartbeatPool.derive(new SpringOutboxTransactions(new PGSimpleDataSource()), interval))
            .as("over a DataSource that is no HikariDataSource").isNull();
        assertThat(HeartbeatPool.derive(new SpringOutboxTransactions(
            new LazyConnectionDataSourceProxy(new PGSimpleDataSource())), interval))
            .as("over a wrapper of a DataSource that is no HikariDataSource").isNull();
    }
}
