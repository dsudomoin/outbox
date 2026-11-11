---
name: testing-outbox
description: Use when writing or fixing tests of a Kora 1.x or Spring Boot service that uses dsudomoin/outbox or its inbox (outbox-spring-boot), such as a @KoraAppTest or @SpringBootTest with Testcontainers Postgres or Kafka that must prove an event was delivered, OutboxRelay.drain(), or existing tests that log Kafka connection errors, hang or slow down after the outbox was added. Also «тест outbox», «интеграционный тест аутбокса», «проверить доставку события».
---

# Testing a service that uses outbox

## Every test that boots the graph

If the test does not assert delivery, its config contains `outbox.relay.enabled = false`. Otherwise each such test
starts relay workers and Kafka producers against a broker that is not there. Keep
`outbox.kafka.driverProperties."bootstrap.servers"` set (it is validated at startup; with the relay off nothing
connects). Add `outbox.partitions { xidsPerPartition = 1000000, premake = 4 }`: startup then creates 6 partitions
instead of 66.

With `outbox.schema.bootstrap = false` startup checks the base tables instead of creating them: run the service's
own migrations against the container before the graph starts (do not switch bootstrap on in tests, it hides a broken
migration). Partitions stay the library's, so the settings above still apply. An `@Tag(OutboxOwner.class)` database
needs its own config section pointing at the same container.

## Delivery test: required parts

1. Postgres Testcontainer of the production major, 14 or newer (no H2). Bulk tests with many distinct keys:
   `.withCommand("postgres", "-c", "max_locks_per_transaction=256")`.
2. Kafka Testcontainer with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, and the topic created with `Admin` **before**
   anything is sent. Auto-create hides a missing deploy step, and a failed send waits in `<table>_retry` for its
   backoff, which `drain()` does not wait for.
3. Config: `relay.enabled = false`, the fast partition settings, `bootstrap.servers` from the container, a unique
   `outbox.table` per test class when classes share one database.
4. Act through the real service method, then `int delivered = relay.drain();` (`@TestComponent OutboxRelay relay`).
   It makes a tick and processes every shard up to it. With the relay on, poll with a deadline; never sleep and then
   assert.
5. Assert on the target, not on outbox rows: the Kafka record's value, key, and
   `KafkaOutboxHeaders.messageId(record)` equal to the id `send` returned, or the HTTP stub's body and
   `Idempotency-Key` equal to that id.
6. Assert `select count(*) from <table>_retry` = 0 (and `<table>_dead` = 0 unless the test expects a dead letter;
   `<table>_parked` = 0 if the handler requeues: `drain()` leaves parked records not yet due).

```java
@KoraAppTest(Application.class)
class OrderDeliveryTest implements KoraAppTestConfigModifier {
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    static { PG.start(); KAFKA.start(); createTopic("orders"); } // Admin.createTopics(...).all().get()

    @TestComponent OrderService orders;
    @TestComponent OutboxRelay relay;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db { jdbcUrl = "%s", username = "%s", password = "%s", poolName = "test" }
                outbox {
                  relay.enabled = false
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                  kafka.driverProperties { "bootstrap.servers" = "%s" }
                }
                """.formatted(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), KAFKA.getBootstrapServers()));
    }

    @Test
    void orderEventReachesKafka() {
        var messageId = orders.createOrder("u1", 100); // the id send returned, if the service exposes it
        assertThat(relay.drain()).isEqualTo(1);
        var record = consumeOne("orders"); // new group, earliest, poll with a deadline
        assertThat(record.key()).isEqualTo("u1");
        assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(messageId); // else: isNotNull()
        assertThat(count("select count(*) from outbox_retry")).isZero();
    }
}
```

Testcontainers 2.x packages: `org.testcontainers.postgresql`, `org.testcontainers.kafka`.

## Spring Boot

The same rules: `outbox.relay.enabled=false`, deliver with `OutboxRelay.drain()`, check the retry table too.

```java
@SpringBootTest(properties = {"outbox.relay.enabled=false", "outbox.partitions.xids-per-partition=1000000",
        "outbox.partitions.premake=4"})
class OrderDeliveryTest {

    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine"); // Testcontainers 2.x

    static {
        PG.start();
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PG::getJdbcUrl);
        registry.add("spring.datasource.username", PG::getUsername);
        registry.add("spring.datasource.password", PG::getPassword);
    }

    @Autowired
    Outbox outbox;

    @Autowired
    OutboxRelay relay;

    @Autowired
    PlatformTransactionManager transactions;

    @Test
    void delivers() {
        new TransactionTemplate(transactions).executeWithoutResult(status ->
            outbox.send(OutboxMessage.of("orders", "u1", new byte[0])));

        assertThat(relay.drain()).isEqualTo(1);
    }
}
```

- No `@Transactional` on the test method: it rolls back, and `drain()` sees nothing.
- A test's `OutboxRelay` field is injected after the context started. A bean injecting it before the start fails; use
  `ObjectProvider<OutboxRelay>` there. A field injected before a `context.stop()`/`start()` keeps the closed relay.
- With `outbox-kafka-spring-boot`, `bootstrap.servers` is needed with the relay off too
  (`outbox.kafka.driverProperties: bootstrap.servers is required`). To assert delivery without Kafka: an `OutboxTopicHandler` bean, or an
  `OutboxHandler` bean with `outbox.kafka.fallback=false`.
- With `outbox.schema.bootstrap=false`, Flyway or Liquibase run at the context's start, before the check. Keep it so in
  tests.

## Other cases

- **Custom handler without Kafka**: a test `@KoraApp` whose `OutboxHandler` factory returns a handler that records
  what it got; `OutboxTopicHandler` components still get their own topics. The library's own test fixtures are not
  published; write the ten lines.
- **Retry path**: with one failing key, `drain()` returns after about 3 pauses with a smaller count, and the record
  waits in `<table>_retry`: assert that count. `drain()` throws `IllegalStateException` only when no shard moves for
  2 × (`batchTimeout` + 5 s), for example when two or more keys fail and nothing is delivered, or when the relay is
  halted. Lower `batchTimeout` only with a non-Kafka handler; with Kafka it must stay ≥
  `delivery.timeout.ms + 5 s + max.block.ms`.
- **Unit tests**: `Outbox` and `Inbox` are interfaces; mock them.
  `new TypedOutbox<>(outbox, serializer, OrderCreated.class)` wraps a mock.
- **Ambiguous `inTx`**: an expression lambda needs a cast, `db.inTx((SqlFunction0<UUID>) () -> outbox.send(m))`.
