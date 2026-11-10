package prodcheck.store;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.admin.spring.OutboxAdminAuthorizer;
import jakarta.servlet.http.HttpServletRequest;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import prodcheck.common.Controls;
import prodcheck.common.HandlerControl;

/** The Spring Boot twin of shop-n: the same routes, topics, contracts and control routes. */
@SpringBootApplication
public class StoreApp {

    static final String TARGET_DB = "targetDb";

    public static void main(String[] args) {
        SpringApplication.run(StoreApp.class, args);
    }

    /** The ledger route's pool on the target database: never a candidate for the outbox's or JPA's DataSource. */
    @Bean(defaultCandidate = false)
    @Qualifier(TARGET_DB)
    HikariDataSource targetDataSource(@Value("${prodcheck.target-db.url}") String url,
            @Value("${prodcheck.target-db.connection-timeout}") Duration connectionTimeout,
            @Value("${prodcheck.target-db.socket-timeout}") String socketTimeout,
            @Value("${prodcheck.instance}") String instance) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("outbox");
        config.setPassword("outbox");
        config.setPoolName("store-target");
        config.setMaximumPoolSize(8);
        config.setConnectionTimeout(connectionTimeout.toMillis());
        config.addDataSourceProperty("ApplicationName", instance);
        config.addDataSourceProperty("socketTimeout", socketTimeout);
        return new HikariDataSource(config);
    }

    /** Kora's {@code db.connectionTimeout} is a duration; Hikari's Boot property takes milliseconds only. */
    @Bean
    static BeanPostProcessor appPoolConnectionTimeout(@Value("${db.connectionTimeout:3s}") Duration timeout) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String name) {
                if (bean instanceof HikariDataSource pool && name.equals("dataSource")) {
                    pool.setConnectionTimeout(timeout.toMillis());
                }
                return bean;
            }
        };
    }

    @Bean
    HandlerControl handlerControl() {
        return new HandlerControl(StoreSender.TOPICS, Set.of());
    }

    @Bean
    Controls controls(DataSource dataSource, PlatformTransactionManager transactions, StoreSender sender,
            HandlerControl handlers, @Value("${prodcheck.instance}") String instance) {
        var db = new SpringJdbc(dataSource, new TransactionTemplate(transactions));
        return new Controls(db, "store", instance, Map.of("main", sender), handlers);
    }

    /** common's ApiKeyAuthorizer on the servlet request: admin may do everything, reader only GET. */
    @Bean
    OutboxAdminAuthorizer outboxAdminAuthorizer(@Value("${outbox-admin.admin-key}") String adminKey,
            @Value("${outbox-admin.reader-key}") String readerKey) {
        if (adminKey.isBlank() || readerKey.isBlank()) {
            throw new IllegalArgumentException("outbox admin keys must not be blank");
        }
        var admin = adminKey.getBytes(UTF_8);
        var reader = readerKey.getBytes(UTF_8);
        return (HttpServletRequest request) -> {
            var header = request.getHeader("X-Api-Key");
            var key = header == null ? new byte[0] : header.getBytes(UTF_8);
            if (MessageDigest.isEqual(key, admin)) {
                return OutboxAdminAuthorizer.allow("admin-key");
            }
            if (MessageDigest.isEqual(key, reader)) {
                return request.getMethod().equals("GET") ? OutboxAdminAuthorizer.allow("reader-key")
                        : OutboxAdminAuthorizer.forbidden("the reader key only reads");
            }
            return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
        };
    }
}
