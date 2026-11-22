package prodcheck.common;

import io.github.dsudomoin.outbox.kora.KoraOutboxConfig;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;

/** For the Kora apps: the outbox config at {@code outbox} is read as {@link ProdcheckOutboxConfig}. */
public interface ProdcheckOutboxModule {

    default ConfigValueExtractor<KoraOutboxConfig> prodcheckOutboxConfigExtractor(
            ConfigValueExtractor<ProdcheckOutboxConfig> tuned) {
        return tuned::extract;
    }
}
