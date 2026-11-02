plugins {
    application
    kotlin("jvm") version "2.1.20"
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
}

val koraVersion: String by project
val outboxVersion: String by project
// plain mirrors the service's graph; full adds metrics and the admin API, installed next to it
val variant = providers.gradleProperty("accountsVariant").getOrElse("plain")
require(variant in setOf("plain", "full")) { "accountsVariant must be plain or full, got $variant" }

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
    implementation("ru.tinkoff.kora:database-jdbc")
    implementation("ru.tinkoff.kora:config-hocon")
    implementation("ru.tinkoff.kora:logging-logback")
    implementation("ru.tinkoff.kora:http-server-undertow")
    implementation("com.google.protobuf:protobuf-java:4.31.1")
    implementation("org.postgresql:postgresql:42.7.7")
    if (variant == "full") {
        implementation("ru.tinkoff.kora:micrometer-module")
        implementation("ru.tinkoff.kora:json-module")
    }
}

kotlin {
    jvmToolchain(21)
    sourceSets.main {
        kotlin.srcDir("src/$variant/kotlin")
    }
}

tasks.installDist {
    destinationDir = layout.buildDirectory.dir(if (variant == "full") "install/accounts-full" else "install/accounts").get().asFile
}

application {
    mainClass = "prodcheck.accounts.AccountsMainKt"
    applicationDefaultJvmArgs = listOf("-Dfile.encoding=UTF-8")
}
