plugins {
    `java-library`
}

val koraVersion: String by project
val outboxVersion: String by project

val koraBom: Configuration by configurations.creating
configurations {
    api.get().extendsFrom(koraBom)
}

dependencies {
    koraBom(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))

    api("io.github.dsudomoin:outbox-core:$outboxVersion")
    api("io.github.dsudomoin:outbox-admin-kora:$outboxVersion")
    api("ru.tinkoff.kora:database-jdbc")
    api("ru.tinkoff.kora:http-server-common")
    api("jakarta.annotation:jakarta.annotation-api")
}
