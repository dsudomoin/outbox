package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.OutboxTransactions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/**
 * JDBC wrappers that add up the length of every {@code byte[]} they return to the thread inside {@link #during}: how
 * many of a row's bytes a call brought into Java. Other threads, a relay's say, read through them uncounted.
 */
public final class ByteReads {

    private static final ThreadLocal<AtomicLong> COUNT = new ThreadLocal<>();
    private static final Set<Class<?>> WRAPPED = Set.of(
            Connection.class, Statement.class, PreparedStatement.class, CallableStatement.class, ResultSet.class);

    private ByteReads() {}

    public static DataSource of(DataSource source) {
        return wrap(DataSource.class, source);
    }

    public static Connection of(Connection c) {
        return wrap(Connection.class, c);
    }

    public static OutboxTransactions of(OutboxTransactions db) {
        return new OutboxTransactions() {
            @Override
            public Connection newConnection() throws SQLException {
                return of(db.newConnection());
            }

            @Override
            public Connection currentConnection() {
                var c = db.currentConnection();
                return c == null ? null : of(c);
            }

            @Override
            public void afterCommit(Runnable action) throws SQLException {
                db.afterCommit(action);
            }

            @Override
            public String transactionHint() {
                return db.transactionHint();
            }

            @Override
            public RuntimeException translate(SQLException error) {
                return db.translate(error);
            }
        };
    }

    /** The bytes read as {@code byte[]} on this thread while {@code body} ran. */
    public static long during(Body body) throws Exception {
        var count = new AtomicLong();
        COUNT.set(count);
        try {
            body.run();
        } finally {
            COUNT.remove();
        }
        return count.get();
    }

    @FunctionalInterface
    public interface Body {
        void run() throws Exception;
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrap(Class<T> type, T target) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            Object result;
            try {
                result = method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            var count = COUNT.get();
            if (count != null && result instanceof byte[] bytes) {
                count.addAndGet(bytes.length);
            }
            var returns = method.getReturnType();
            return result != null && WRAPPED.contains(returns) ? wrap((Class<Object>) returns, result) : result;
        });
    }
}
