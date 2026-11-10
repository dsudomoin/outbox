plugins {
    application
}

val outboxVersion: String by project

// One source, two Boot lines side by side: -PbootLine=3.5 or 4.1 (the versions the library's own build tests)
val bootLines = mapOf("3.5" to "3.5.16", "4.1" to "4.1.2")
val bootLine = (findProperty("bootLine") as String?) ?: "4.1"
val bootVersion = bootLines[bootLine] ?: error("bootLine must be one of ${bootLines.keys}, got $bootLine")
val boot4 = bootLine == "4.1"

dependencies {
    // enforced: common brings Kora's BOM, and Boot's versions must win for everything Boot manages
    implementation(enforcedPlatform("org.springframework.boot:spring-boot-dependencies:$bootVersion"))
    implementation(project(":common")) {
        exclude(group = "io.github.dsudomoin", module = "outbox-admin-kora") // only ApiKeyAuthorizer, Kora's
    }
    implementation("io.github.dsudomoin:outbox-spring-boot:$outboxVersion")
    implementation("io.github.dsudomoin:outbox-admin-spring-boot:$outboxVersion")
    implementation(if (boot4) "org.springframework.boot:spring-boot-starter-webmvc" else "org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("org.postgresql:postgresql")
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-parameters")
}

application {
    mainClass = "prodcheck.store.StoreApp"
    applicationDefaultJvmArgs = listOf("-Dfile.encoding=UTF-8")
}

tasks.installDist {
    destinationDir = layout.buildDirectory.dir("install/store-boot${bootLine.replace(".", "")}").get().asFile
}
