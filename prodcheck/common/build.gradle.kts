plugins {
    `java-library`
}

val koraVersion: String by project
val outboxVersion: String by project

val koraBom: Configuration by configurations.creating
configurations {
    api.get().extendsFrom(koraBom)
    annotationProcessor.get().extendsFrom(koraBom)
}

dependencies {
    koraBom(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
    annotationProcessor("ru.tinkoff.kora:config-annotation-processor") // ProdcheckOutboxConfig's extractor

    api("io.github.dsudomoin:outbox-core:$outboxVersion")
    api("io.github.dsudomoin:outbox-admin-kora:$outboxVersion")
    compileOnly("io.github.dsudomoin:outbox-kora:$outboxVersion") // ProdcheckOutboxConfig, for the Kora apps only
    api("ru.tinkoff.kora:database-jdbc")
    api("ru.tinkoff.kora:http-server-common")
    api("jakarta.annotation:jakarta.annotation-api")
}

// The library's Tuning (its internal values, which the scenarios set from HOCON) came with the settings cut; built
// against a library from before it, the apps get ProdcheckOutboxModule without it
val outboxCore = configurations.detachedConfiguration(dependencies.create("io.github.dsudomoin:outbox-core:$outboxVersion"))
    .apply { isTransitive = false }
val tuning = !zipTree(outboxCore.singleFile).matching { include("io/github/dsudomoin/outbox/internal/Tuning.class") }.isEmpty
sourceSets.main {
    java.srcDir(if (tuning) "src/tuning/java" else "src/untuned/java")
}
