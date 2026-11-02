plugins {
    application
    kotlin("jvm") version "1.9.25"
    id("com.google.devtools.ksp") version "1.9.25-1.0.20"
}

val koraVersion: String by project
val outboxVersion: String by project

val koraBom: Configuration by configurations.creating
configurations {
    ksp.get().extendsFrom(koraBom)
    implementation.get().extendsFrom(koraBom)
}

dependencies {
    koraBom(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
    ksp("ru.tinkoff.kora:symbol-processors")

    implementation(project(":common"))
    implementation("io.github.dsudomoin:outbox-kora:$outboxVersion")
    implementation("io.github.dsudomoin:outbox-micrometer:$outboxVersion")
    implementation("ru.tinkoff.kora:database-jdbc")
    implementation("ru.tinkoff.kora:config-hocon")
    implementation("ru.tinkoff.kora:logging-logback")
    implementation("ru.tinkoff.kora:http-server-undertow")
    implementation("ru.tinkoff.kora:micrometer-module")
    implementation("ru.tinkoff.kora:json-module")
    implementation("org.postgresql:postgresql:42.7.14")
}

application {
    mainClass = "prodcheck.courier.CourierAppKt"
    applicationDefaultJvmArgs = listOf("-Dfile.encoding=UTF-8")
}
