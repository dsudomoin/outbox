package prodcheck.common;

import io.github.dsudomoin.outbox.internal.Tuning;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/**
 * The outbox's config at {@code outbox} with the library's internal {@link Tuning} read from it too, its keys right
 * under {@code outbox} ({@code outbox.maintenanceInterval}, {@code outbox.shards}): the scenarios vary values that no
 * production app can set.
 */
@ConfigValueExtractor
public interface ProdcheckOutboxConfig extends KoraOutboxConfig, Tuning {}
