package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transactions over a plain {@link DataSource}, for code without a framework: {@link #inTx} runs a call in a
 * transaction bound to the calling thread, in which {@code Outbox.send} and the inbox join it.
 * <pre>{@code
 * var transactions = new JdbcTransactions(dataSource);
 * transactions.inTx(c -> {
 *     orders.insert(c, order);
 *     return outbox.send(OutboxMessage.of("orders", order.userId(), payload));
 * });
 * }</pre>
 */
public final class JdbcTransactions implements OutboxTransactions {

    private static final Logger log = LoggerFactory.getLogger(JdbcTransactions.class);

    private final DataSource dataSource;
    private final ThreadLocal<Transaction> current = new ThreadLocal<>();

    public JdbcTransactions(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Runs {@code call} in a transaction and commits it, then runs its after-commit actions; any throwable rolls it
     * back and reaches the caller, an {@link SQLException} as {@link OutboxSqlException}. Once the commit is
     * acknowledged nothing else fails the call: a failed reset or close of the connection is a WARN, and the actions
     * run. A failed rollback aborts the connection, whose session then ends without committing. Inside another
     * {@code inTx} on this thread, {@code call} joins that transaction.
     */
    public <T> T inTx(SqlCall<T> call) {
        var outer = current.get();
        if (outer != null) {
            try {
                return call.call(outer.connection);
            } catch (SQLException e) {
                throw translate(e);
            }
        }
        Connection c;
        try {
            c = dataSource.getConnection();
        } catch (SQLException e) {
            throw translate(e);
        }
        var tx = new Transaction(c);
        current.set(tx);
        T result;
        try {
            c.setAutoCommit(false);
            result = call.call(c);
            c.commit();
        } catch (SQLException e) {
            current.remove();
            release(c, true, suppressedInto(e));
            throw translate(e);
        } catch (Throwable t) {
            current.remove();
            release(c, true, suppressedInto(t));
            throw t;
        }
        current.remove();
        release(c, false, e -> log.warn("outbox connection failed its cleanup after a commit; the transaction is"
                + " committed", e));
        for (var action : tx.afterCommit) {
            try {
                action.run();
            } catch (RuntimeException e) { // committed: the caller must not take it for a failure and retry
                log.warn("outbox after-commit action failed; the transaction is committed", e);
            }
        }
        return result;
    }

    @Nullable
    @Override
    public Connection currentConnection() {
        var tx = current.get();
        return tx == null ? null : tx.connection;
    }

    @Override
    public void afterCommit(Runnable action) {
        var tx = current.get();
        if (tx == null) {
            throw new IllegalStateException("not inside JdbcTransactions.inTx(...)");
        }
        for (var registered : tx.afterCommit) {
            if (registered == action) {
                return;
            }
        }
        tx.afterCommit.add(action);
    }

    @Override
    public Connection newConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public String transactionHint() {
        return "JdbcTransactions.inTx(...)";
    }

    /**
     * Ends {@code c}'s use: rolls back when {@code rollback}, turns autocommit on and closes it. A connection whose
     * rollback or reset fails is aborted instead, as its state is unknown: with its session alive, turning autocommit
     * on would commit what the rollback left, and a pool would hand it out again. {@code failed} gets every failure,
     * an {@link Error} too, so none replaces the transaction's outcome, which is known by now, or leaks the connection.
     */
    private static void release(Connection c, boolean rollback, Consumer<Throwable> failed) {
        try {
            if (rollback) {
                c.rollback();
            }
            c.setAutoCommit(true);
        } catch (Throwable e) {
            failed.accept(e);
            try {
                c.abort(Runnable::run);
            } catch (Throwable aborting) {
                failed.accept(aborting);
            }
        }
        try {
            c.close();
        } catch (Throwable e) {
            failed.accept(e);
        }
    }

    /** Adds a cleanup's failure to {@code failure}, unless a driver threw that very exception again. */
    private static Consumer<Throwable> suppressedInto(Throwable failure) {
        return e -> {
            if (e != failure) {
                failure.addSuppressed(e);
            }
        };
    }

    /** A call in a transaction, on its connection. */
    @FunctionalInterface
    public interface SqlCall<T> {
        T call(Connection connection) throws SQLException;
    }

    private static final class Transaction {
        final Connection connection;
        final List<Runnable> afterCommit = new ArrayList<>(1);

        Transaction(Connection connection) {
            this.connection = connection;
        }
    }
}
