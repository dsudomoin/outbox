package prodcheck.shop;

import ru.tinkoff.kora.json.common.annotation.Json;

@Json
public record OrderCreatedV1(long seq, String tag, String pad) {}
