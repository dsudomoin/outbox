package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraDeadLettersConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraInboxConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraPartitionsConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraRecoveryConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraRelayConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraRequeueConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraRetryConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraSchemaConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraTailConversionConfig;
import io.github.dsudomoin.outbox.kora.KoraOutboxConfig.KoraWaitConfig;

/**
 * One shared instance per nested block a HOCON leaves out. Kora's graph refresh stops at a component whose new value
 * equals the old one; the generated config records compare their blocks with {@code equals}, so a fresh anonymous
 * default on every extraction would make any reload of the config file rebuild the outbox.
 */
final class KoraConfigDefaults {

    static final KoraRelayConfig RELAY = new KoraRelayConfig() {};
    static final KoraRetryConfig RETRY = new KoraRetryConfig() {};
    static final KoraRequeueConfig REQUEUE = new KoraRequeueConfig() {};
    static final KoraWaitConfig WAITING = new KoraWaitConfig() {};
    static final KoraPartitionsConfig PARTITIONS = new KoraPartitionsConfig() {};
    static final KoraTailConversionConfig TAIL_CONVERSION = new KoraTailConversionConfig() {};
    static final KoraInboxConfig INBOX = new KoraInboxConfig() {};
    static final KoraDeadLettersConfig DEAD_LETTERS = new KoraDeadLettersConfig() {};
    static final KoraRecoveryConfig RECOVERY = new KoraRecoveryConfig() {};
    static final KoraSchemaConfig SCHEMA = new KoraSchemaConfig() {};

    private KoraConfigDefaults() {}
}
