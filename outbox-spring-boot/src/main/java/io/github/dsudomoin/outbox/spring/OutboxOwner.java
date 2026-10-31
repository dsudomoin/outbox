package io.github.dsudomoin.outbox.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * The {@code DataSource} of the role that owns the outbox's tables: the outbox's and the inbox's DDL, session locks
 * and recoveries run on its connections, everything else on the application's DataSource, which then needs DML only.
 * Requires {@code outbox.schema.bootstrap=false}: the application's migrations own the base tables. Declared as no
 * default candidate, so Boot keeps its own DataSource and nothing else injects this one:
 * <pre>{@code
 * @Bean(defaultCandidate = false)
 * @OutboxOwner
 * DataSource outboxOwnerDataSource() {
 *     return DataSourceBuilder.create().url(url).username("outbox_owner").password(password).build();
 * }
 * }</pre>
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Qualifier
public @interface OutboxOwner {}
