package prodcheck.store;

/** What shop-n writes on {@code orders}: contract 1, as Kora's {@code @Json} writes it. */
public record OrderCreatedV1(long seq, String tag, String pad) {}
