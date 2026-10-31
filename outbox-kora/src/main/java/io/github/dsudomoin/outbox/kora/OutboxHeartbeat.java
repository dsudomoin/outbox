package io.github.dsudomoin.outbox.kora;

/**
 * The tag of a {@code JdbcConnectionFactory} for the outbox's heartbeat, which keeps this instance's row fresh and
 * reads the reports that guard routed sends: with it, the beats take their connections from it, whatever
 * {@code outbox.heartbeat.dedicatedConnection} says, so an app can point them elsewhere: past a pooler, as another
 * role, with other timeouts. It must reach the app's database; one connection is all it needs, and its connection
 * timeout should stay well below {@code outbox.partitions.maintenanceInterval}.
 * <pre>{@code
 * @Tag(OutboxHeartbeat.class)
 * default JdbcDatabase outboxHeartbeatDatabase(Config config, ConfigValueExtractor<JdbcDatabaseConfig> extractor,
 *         DataBaseTelemetryFactory telemetry) {
 *     return new JdbcDatabase(extractor.extract(config.get("heartbeatDb")), telemetry);
 * }
 * }</pre>
 */
public final class OutboxHeartbeat {

    private OutboxHeartbeat() {}
}
