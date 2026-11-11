package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxConfig;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/**
 * {@link OutboxConfig} read from HOCON path {@code outbox}: the same keys and defaults; every key is optional. The
 * nested blocks are their Kora counterparts, so Kora's config processor reads each of them too.
 */
@ConfigValueExtractor
public interface KoraOutboxConfig extends OutboxConfig {

    @Override
    default KoraRelayConfig relay() {
        return KoraConfigDefaults.RELAY;
    }

    @Override
    default KoraPartitionsConfig partitions() {
        return KoraConfigDefaults.PARTITIONS;
    }

    @Override
    default KoraInboxConfig inbox() {
        return KoraConfigDefaults.INBOX;
    }

    @Override
    default KoraDeadLettersConfig deadLetters() {
        return KoraConfigDefaults.DEAD_LETTERS;
    }

    @Override
    default KoraRecoveryConfig recovery() {
        return KoraConfigDefaults.RECOVERY;
    }

    @Override
    default KoraSchemaConfig schema() {
        return KoraConfigDefaults.SCHEMA;
    }

    @ConfigValueExtractor
    interface KoraRelayConfig extends RelayConfig {

        @Override
        default KoraRetryConfig retry() {
            return KoraConfigDefaults.RETRY;
        }

        @Override
        default KoraRequeueConfig requeue() {
            return KoraConfigDefaults.REQUEUE;
        }

        @Override
        default KoraWaitConfig waiting() {
            return KoraConfigDefaults.WAITING;
        }
    }

    @ConfigValueExtractor
    interface KoraRetryConfig extends RetryConfig {}

    @ConfigValueExtractor
    interface KoraRequeueConfig extends RequeueConfig {}

    @ConfigValueExtractor
    interface KoraWaitConfig extends WaitConfig {}

    @ConfigValueExtractor
    interface KoraPartitionsConfig extends PartitionsConfig {

        @Override
        default KoraTailConversionConfig tailConversion() {
            return KoraConfigDefaults.TAIL_CONVERSION;
        }
    }

    @ConfigValueExtractor
    interface KoraTailConversionConfig extends TailConversionConfig {}

    @ConfigValueExtractor
    interface KoraInboxConfig extends InboxConfig {}

    @ConfigValueExtractor
    interface KoraDeadLettersConfig extends DeadLettersConfig {}

    @ConfigValueExtractor
    interface KoraRecoveryConfig extends RecoveryConfig {}

    @ConfigValueExtractor
    interface KoraSchemaConfig extends SchemaConfig {}
}
