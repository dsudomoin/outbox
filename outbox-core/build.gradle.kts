plugins {
    `java-test-fixtures`
}

// No framework here: verifyFrameworkFree fails the build when Kora, Micrometer or OpenTelemetry reach the classpath.
dependencies {
    implementation("org.slf4j:slf4j-api:2.0.17")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")
    // optional: ProtobufOutbox needs it on the app's classpath
    compileOnly("com.google.protobuf:protobuf-java:3.25.8")
    // optional: HeartbeatPoolConfig, for the adapters that derive the heartbeat pool from a Hikari one
    compileOnly("com.zaxxer:HikariCP:7.1.0")

    testFixturesApi("org.testcontainers:testcontainers-postgresql:2.0.5")
    testFixturesApi("org.postgresql:postgresql:42.7.14")
    testFixturesCompileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testImplementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
    testImplementation("com.google.protobuf:protobuf-java:3.25.8")
    testImplementation("com.zaxxer:HikariCP:7.1.0")
}
