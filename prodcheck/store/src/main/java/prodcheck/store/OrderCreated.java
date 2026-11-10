package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxContract;
import jakarta.annotation.Nullable;

/** shop-n's reader type: reads v1 and v2. */
@OutboxContract(2)
public record OrderCreated(long seq, String tag, String pad, @Nullable String currency) {}
