package io.github.dsudomoin.outbox.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * The {@code DataSource} of the outbox's heartbeat, which keeps this instance's row fresh and reads the reports that
 * guard routed sends. With such a bean, the beats take their connections from it, whatever
 * {@code outbox.heartbeat.dedicated-connection} says. An app can so point them past a pooler, run them as another
 * role or give them other timeouts. It must reach the app's database. Declared as no default candidate, so Boot keeps
 * its own DataSource and nothing else injects this one; one connection is all it needs, and its connection timeout
 * should stay well below {@code outbox.partitions.maintenance-interval}:
 * <pre>{@code
 * @Bean(defaultCandidate = false)
 * @OutboxHeartbeat
 * DataSource outboxHeartbeatDataSource() {
 *     var pool = DataSourceBuilder.create().type(HikariDataSource.class)
 *             .url(directUrl).username(username).password(password).build();
 *     pool.setPoolName("outbox-heartbeat-direct");
 *     pool.setMaximumPoolSize(1); // Hikari keeps 10 connections open otherwise
 *     pool.setConnectionTimeout(5_000);
 *     return pool;
 * }
 * }</pre>
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Qualifier
public @interface OutboxHeartbeat {}
