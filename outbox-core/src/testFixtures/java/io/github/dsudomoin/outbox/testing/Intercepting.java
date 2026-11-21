package io.github.dsudomoin.outbox.testing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import javax.sql.DataSource;

/** A {@link DataSource} whose connections run a test's hook ahead of each call. */
public final class Intercepting {

    private Intercepting() {}

    /** What runs ahead of a connection's {@code call}; it may throw in its place. */
    @FunctionalInterface
    public interface ConnectionCall {
        void run(Connection real, Method call, Object[] args) throws Throwable;
    }

    /** {@code source}, a plain wrapper restoring nothing, with {@code before} run ahead of each connection's call. */
    public static DataSource intercepting(DataSource source, ConnectionCall before) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (ds, method, args) -> {
                    var result = invoke(source, method, args);
                    return !method.getName().equals("getConnection") ? result : connection((Connection) result, before);
                });
    }

    /** {@code real} with {@code before} run ahead of each of its calls. */
    public static Connection connection(Connection real, ConnectionCall before) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (c, call, values) -> {
                    before.run(real, call, values);
                    return invoke(real, call, values);
                });
    }

    public static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
