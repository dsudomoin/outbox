package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.internal.Tuning;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/**
 * {@link KoraOutboxConfig} with the library's {@link Tuning} read from HOCON too, its keys right under {@code outbox}
 * ({@code outbox.shards}, {@code outbox.minTickInterval}): for test apps only, which extend {@link TunedKoraModule}.
 */
@ConfigValueExtractor
public interface TunedKoraConfig extends KoraOutboxConfig, Tuning {}
