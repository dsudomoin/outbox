package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.kora.KoraOutboxConfig;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;

/**
 * For test apps: the outbox config at {@code outbox} is read as {@link TunedKoraConfig}, so their HOCON sets the
 * library's {@code Tuning} as well, which no production app can.
 */
public interface TunedKoraModule {

    default ConfigValueExtractor<KoraOutboxConfig> tunedOutboxConfigExtractor(
            ConfigValueExtractor<TunedKoraConfig> tuned) {
        return tuned::extract;
    }
}
