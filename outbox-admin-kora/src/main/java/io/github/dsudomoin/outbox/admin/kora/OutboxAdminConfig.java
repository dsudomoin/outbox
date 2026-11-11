package io.github.dsudomoin.outbox.admin.kora;

import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/** The admin API's config, read from HOCON path {@code outbox.admin}; every key is optional. */
@ConfigValueExtractor
public interface OutboxAdminConfig {

    /**
     * Whether a call other than a {@code GET} or {@code HEAD} without {@code Sec-Fetch-Site} is refused when its
     * {@code Origin} is {@code null} or names another host and port than the request's {@code Host}. {@code false}
     * for a proxy that rewrites {@code Host} where browsers reach the API over plain HTTP; {@code Sec-Fetch-Site} is
     * checked either way.
     */
    default boolean checkOrigin() {
        return true;
    }
}
