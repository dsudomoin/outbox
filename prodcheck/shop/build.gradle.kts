plugins {
    application
}

val koraVersion: String by project
val outboxVersion: String by project
val release: String by project

val koraBom: Configuration by configurations.creating
configurations {
    annotationProcessor.get().extendsFrom(koraBom)
    implementation.get().extendsFrom(koraBom)
}

dependencies {
    koraBom(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
    annotationProcessor("ru.tinkoff.kora:annotation-processors")

    implementation(project(":common"))
    implementation("io.github.dsudomoin:outbox-kora:$outboxVersion")
    implementation("io.github.dsudomoin:outbox-micrometer:$outboxVersion")
    implementation("ru.tinkoff.kora:database-jdbc")
    implementation("ru.tinkoff.kora:config-hocon")
    implementation("ru.tinkoff.kora:logging-logback")
    implementation("ru.tinkoff.kora:http-server-undertow")
    implementation("ru.tinkoff.kora:http-client-ok")
    implementation("ru.tinkoff.kora:micrometer-module")
    implementation("ru.tinkoff.kora:json-module")
    implementation("org.postgresql:postgresql:42.7.14")
}

require(release in setOf("n1", "n", "n2")) { "release must be n1, n or n2, got $release" }
sourceSets.main {
    java.srcDir("src/release-$release/java")
}

application {
    mainClass = "prodcheck.shop.ShopApp"
    applicationDefaultJvmArgs = listOf("-Dfile.encoding=UTF-8")
}

// One install per release (and library version), side by side: scenarios run them against each other.
tasks.installDist {
    val defaultVersion = "0.1.0-SNAPSHOT"
    val suffix = if (outboxVersion == defaultVersion) release else "$release-$outboxVersion"
    destinationDir = layout.buildDirectory.dir("install/shop-$suffix").get().asFile
}
