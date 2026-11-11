package io.github.dsudomoin.outbox.admin.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The admin API's settings under {@code outbox.admin.*} (kebab-case canonical, camelCase binds too).
 *
 * @param checkOrigin true: a call other than a GET or HEAD without Sec-Fetch-Site is refused when its Origin is null
 *     or names another host and port than the request's server name and port. false: Origin is not looked at, for a
 *     proxy that rewrites Host where browsers reach the API over plain HTTP; Sec-Fetch-Site is checked either way.
 *     Default true.
 */
@ConfigurationProperties("outbox.admin")
public record OutboxAdminProperties(@DefaultValue("true") boolean checkOrigin) {
}
