package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.support.SQLExceptionSubclassTranslator;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Spring-managed transactions over one {@link DataSource}: {@code Outbox.send} and the inbox inside
 * {@code @Transactional} or a {@code TransactionTemplate} write on the JDBC connection the transaction manager bound
 * for this DataSource ({@code DataSourceTransactionManager}, {@code JdbcTransactionManager},
 * {@code JpaTransactionManager}), in one PostgreSQL transaction with the business writes. Outside a transaction, or
 * inside a transaction of another DataSource, they are refused; this class never opens a transaction or fetches a
 * connection for them.
 */
public final class SpringOutboxTransactions implements OutboxTransactions {

    private static final Logger log = LoggerFactory.getLogger(SpringOutboxTransactions.class);

    private final DataSource dataSource;
    private final ConnectionSource connections;
    private final SQLExceptionSubclassTranslator translator = new SQLExceptionSubclassTranslator();

    public SpringOutboxTransactions(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.connections = autocommit(dataSource);
    }

    /**
     * New connections of {@code dataSource}, switched to autocommit when the pool hands them without it (as
     * {@code spring.datasource.hikari.auto-commit=false} does), as {@link ConnectionSource#newConnection()} requires;
     * never the connection of the caller's transaction.
     */
    public static ConnectionSource autocommit(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        return () -> {
            var c = dataSource.getConnection();
            try {
                if (!c.getAutoCommit()) {
                    c.setAutoCommit(true);
                }
            } catch (SQLException e) {
                try {
                    c.close();
                } catch (SQLException close) {
                    e.addSuppressed(close);
                }
                throw e;
            }
            return c;
        };
    }

    /** The connection bound for this DataSource by the actual transaction on this thread; null without one. */
    @Nullable
    @Override
    public Connection currentConnection() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            return null;
        }
        return TransactionSynchronizationManager.getResource(dataSource) instanceof ConnectionHolder holder
            && holder.getConnectionHandle() != null ? holder.getConnection() : null;
    }

    /** A transaction synchronization; equal for one action, so Spring runs it once however often registered. */
    @Override
    public void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new AfterCommit(action));
    }

    @Override
    public Connection newConnection() throws SQLException {
        return connections.newConnection();
    }

    @Override
    public String transactionHint() {
        return "@Transactional or TransactionTemplate";
    }

    /** Spring's {@link DataAccessException}: a failed send rolls the surrounding transaction back as any other. */
    @Override
    public RuntimeException translate(SQLException error) {
        DataAccessException translated = translator.translate("outbox", null, error);
        return translated != null ? translated : new UncategorizedSQLException("outbox", null, error);
    }

    private record AfterCommit(Runnable action) implements TransactionSynchronization {

        @Override
        public void afterCommit() {
            try {
                action.run();
            } catch (RuntimeException e) { // committed: the caller must not take it for a failure and retry
                log.warn("outbox after-commit action failed; the transaction is committed", e);
            }
        }
    }
}
