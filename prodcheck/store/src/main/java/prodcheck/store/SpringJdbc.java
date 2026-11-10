package prodcheck.store;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionTemplate;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * The harness's {@code inTx} over a Spring transaction: the callback gets the connection the transaction manager bound
 * to the DataSource, the one {@code Outbox.send} and JPA write on, so the journal rows share their transaction.
 */
final class SpringJdbc implements JdbcConnectionFactory {

    private final DataSource dataSource;
    private final TransactionTemplate transaction;

    SpringJdbc(DataSource dataSource, TransactionTemplate transaction) {
        this.dataSource = dataSource;
        this.transaction = transaction;
    }

    @Override
    public <T> T inTx(JdbcHelper.SqlFunction1<Connection, T> callback) {
        return transaction.execute(status -> withConnection(c -> {
            if (!DataSourceUtils.isConnectionTransactional(c, dataSource)) {
                throw new IllegalStateException("the transaction manager bound no connection of this DataSource");
            }
            return callback.apply(c);
        }));
    }

    /** The transaction's connection inside one, else one in autocommit. */
    @Override
    public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
        var c = DataSourceUtils.getConnection(dataSource);
        try {
            return callback.apply(c);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        } finally {
            DataSourceUtils.releaseConnection(c, dataSource);
        }
    }

    @Override
    public Connection currentConnection() {
        throw new UnsupportedOperationException("the harness only uses inTx");
    }

    @Override
    public Connection newConnection() {
        throw new UnsupportedOperationException("the harness only uses inTx");
    }

    @Override
    public DataBaseTelemetry telemetry() {
        throw new UnsupportedOperationException("the harness only uses inTx");
    }
}
