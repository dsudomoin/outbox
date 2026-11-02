package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxContract;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.json.common.annotation.Json;

@OutboxContract(2)
@Json
public record OrderCreated(long seq, String tag, String pad, @Nullable String currency) {}
