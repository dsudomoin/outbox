val koraVersion: String by project

val published = listOf(
    "outbox-core", "outbox-kafka", "outbox-micrometer", "outbox-kora", "outbox-kafka-kora", "outbox-admin-kora",
    "outbox-spring-boot", "outbox-kafka-spring-boot",
)

configure(published.map { project(":$it") }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    repositories {
        mavenCentral()
        google { content { includeGroupByRegex("com\\.android\\.tools.*|androidx\\..*") } } // Metalava only
    }

    configure<JavaPluginExtension> {
        toolchain.languageVersion = JavaLanguageVersion.of(17)
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.12.2"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testImplementation"("org.assertj:assertj-core:3.27.6")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.17")
    }

    // -Pbenchmark runs only the tests tagged "benchmark" (never part of a normal build), always rerun, their stdout
    // shown; -Dbench.* and -Doutbox.test.* properties of the Gradle invocation reach the test JVM. On CI (the CI
    // environment variable set) the tests tagged "timing", whose property is a speed a runner of unknown power may not
    // reach, are left out unless -Poutbox.test.timing asks for them
    val benchmark = project.hasProperty("benchmark")
    val timing = System.getenv("CI") == null || project.hasProperty("outbox.test.timing")
    tasks.withType<Test>().configureEach {
        useJUnitPlatform {
            if (benchmark) includeTags("benchmark") else excludeTags("benchmark")
            if (!timing) excludeTags("timing")
        }
        // outbox-kora holds most of the suite: its classes go round-robin to several JVMs, each with its own Postgres and
        // load tests that want cores of their own, so one JVM per 3 cores, at most 6. A small CI runner keeps one, as
        // before; --max-workers caps them too. Benchmarks keep the machine to themselves.
        maxParallelForks = if (benchmark || project.name != "outbox-kora") 1
            else (Runtime.getRuntime().availableProcessors() / 3).coerceIn(1, 6)
        if (benchmark) {
            outputs.upToDateWhen { false }
            testLogging.events("standard_out")
        }
        System.getProperties().forEach { (k, v) ->
            if (k.toString().startsWith("bench.") || k.toString().startsWith("outbox.test.")) {
                systemProperty(k.toString(), v)
            }
        }
    }

    // -Poutbox.test.kafkaClients, .micrometer, .micrometerTracing and .opentelemetry run the tests on that version:
    // forced on the test runtime classpath only, so the code still compiles against the declared versions
    val kafkaClients = findProperty("outbox.test.kafkaClients") as String?
    val micrometer = findProperty("outbox.test.micrometer") as String?
    val micrometerTracing = findProperty("outbox.test.micrometerTracing") as String?
    val opentelemetry = findProperty("outbox.test.opentelemetry") as String?
    configurations.named("testRuntimeClasspath") {
        resolutionStrategy.eachDependency {
            val forced = when {
                requested.group == "org.apache.kafka" && requested.name == "kafka-clients" -> kafkaClients
                requested.group == "io.micrometer" && requested.name.startsWith("micrometer-tracing") -> micrometerTracing
                requested.group == "io.micrometer" && requested.name.startsWith("micrometer-") -> micrometer
                // the -alpha artifacts are versioned apart from the stable ones
                requested.group == "io.opentelemetry" && requested.version?.endsWith("-alpha") != true -> opentelemetry
                else -> null
            }
            if (forced != null) useVersion(forced)
        }
    }

    // The public API as Metalava's signature of the main sources, without the internal and generated packages and
    // @InternalApi members: apiDump writes api/<module>.txt, apiCheck fails when the sources no longer match it
    val metalava = configurations.create("metalava") { isCanBeConsumed = false }
    dependencies { "metalava"("com.android.tools.metalava:metalava:1.0.0-alpha16") }
    val toolchains = the<JavaToolchainService>()
    val jdk = toolchains.launcherFor(the<JavaPluginExtension>().toolchain) // the JDK the API is compiled against
    val main = the<SourceSetContainer>()["main"]
    val sources = main.java.sourceDirectories // with the generated ones, so their tasks run first
    val compileClasspath = main.compileClasspath
    val excluded = listOf("internal", "admin.kora.api", "admin.kora.model").map { "io.github.dsudomoin.outbox.$it" }
    fun signatureTask(name: String, out: Provider<RegularFile>) = tasks.register<JavaExec>(name) {
        classpath = metalava
        mainClass = "com.android.tools.metalava.Driver"
        javaLauncher = toolchains.launcherFor { languageVersion = JavaLanguageVersion.of(21) } // Metalava needs 21
        inputs.files(sources).withPathSensitivity(PathSensitivity.RELATIVE)
        inputs.files(compileClasspath).withNormalizer(CompileClasspathNormalizer::class)
        outputs.file(out)
        argumentProviders.add {
            listOf(
                "main", "--format=4.0", "--api", out.get().asFile.path, "--java-source", "17",
                "--jdk-home", jdk.get().metadata.installationPath.asFile.path,
                "--source-path", sources.filter { it.exists() }.asPath,
                "--classpath", compileClasspath.filter { it.exists() }.asPath,
                "--stub-packages", (listOf("*") + excluded.map { "-$it.*" }).joinToString(":"),
                "--hide-annotation", "io.github.dsudomoin.outbox.InternalApi",
                "--hide", "AddedSealed", // Metalava does not model sealed types yet; they are listed as plain ones
            )
        }
        doLast { // Metalava lists a public member that names an excluded type instead of refusing it
            val leaks = out.get().asFile.readLines().filter { line -> excluded.any { "$it." in line } }
            if (leaks.isNotEmpty()) {
                throw GradleException("The public API names excluded types; mark these members @InternalApi:\n" +
                    leaks.joinToString("\n"))
            }
        }
    }
    val signature = layout.projectDirectory.file("api/${project.name}.txt")
    val apiDump = signatureTask("apiDump", provider { signature })
    apiDump { description = "Writes the public API to ${signature.asFile.name}" }
    val current = layout.buildDirectory.file("api/${project.name}.txt")
    val apiCheck = signatureTask("apiCheck", current)
    apiCheck {
        description = "Fails when the public API differs from ${signature.asFile.name} (apiDump updates it)"
        inputs.file(signature)
        mustRunAfter(apiDump)
        doLast {
            val expected = signature.asFile.readLines()
            val actual = current.get().asFile.readLines()
            if (actual != expected) { // shows the lines between the first and the last difference
                val head = expected.zip(actual).takeWhile { (e, a) -> e == a }.size
                val tail = expected.drop(head).asReversed().zip(actual.drop(head).asReversed())
                    .takeWhile { (e, a) -> e == a }.size
                val diff = expected.subList(head, expected.size - tail).map { "- $it" } +
                    actual.subList(head, actual.size - tail).map { "+ $it" }
                throw GradleException("${signature.asFile} differs from the public API at line ${head + 1}" +
                    " (./gradlew apiDump updates it):\n" + diff.joinToString("\n"))
            }
        }
    }
    tasks.named("check") { dependsOn(apiCheck) }

    configure<PublishingExtension> {
        publications.create<MavenPublication>("maven") {
            artifactId = project.name
            from(components["java"])
            pom {
                name = project.name
                description = when {
                    project.name.endsWith("-kora") -> "Transactional outbox for PostgreSQL — Kora 1.x adapter"
                    project.name.endsWith("-spring-boot") ->
                        "Transactional outbox for PostgreSQL — Spring Boot 3.5/4.1 adapter"
                    else -> "Transactional outbox for PostgreSQL"
                }
                url = "https://github.com/dsudomoin/outbox"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/licenses/MIT"
                    }
                }
                scm {
                    url = "https://github.com/dsudomoin/outbox"
                    connection = "scm:git:https://github.com/dsudomoin/outbox.git"
                }
            }
        }
    }
}

// Kora's platform and config processor reach the Kora adapters only: the core, Kafka and Micrometer modules stay
// framework-free (verifyFrameworkFree)
configure(listOf("outbox-kora", "outbox-kafka-kora", "outbox-admin-kora").map { project(":$it") }) {
    dependencies {
        "api"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "testAnnotationProcessor"(platform("ru.tinkoff.kora:kora-parent:$koraVersion"))
        "annotationProcessor"("ru.tinkoff.kora:config-annotation-processor")
    }
}

// The Spring Boot adapters: one jar for Boot 3.5 and 4.1, compiled against Boot 3.5 with Spring and Boot compileOnly
// (the application brings its Boot). Their tests run twice: `test` on Boot 3.5, `testBoot4` on Boot 4.1, the same test
// classes, compiled against Boot 3.5. A dependency both runs need goes to testImplementation, version-less when Boot
// manages it (each run's platform picks the version); one run's only to boot35Test or boot41Test.
val bootLines = mapOf("3.5" to "3.5.16", "4.1" to "4.1.2")
val springBootModules = listOf("outbox-spring-boot", "outbox-kafka-spring-boot") // outbox-admin-spring-boot joins here
configure(springBootModules.map { project(":$it") }) {
    fun bootPlatform(line: String) =
        dependencies.platform("org.springframework.boot:spring-boot-dependencies:${bootLines[line]}")
    val boot35Test = configurations.create("boot35Test") { isCanBeConsumed = false; isCanBeResolved = false }
    val boot41Test = configurations.create("boot41Test") { isCanBeConsumed = false; isCanBeResolved = false }
    configurations["testCompileClasspath"].extendsFrom(boot35Test)
    configurations["testRuntimeClasspath"].extendsFrom(boot35Test)
    val testBoot4RuntimeClasspath = configurations.create("testBoot4RuntimeClasspath") {
        isCanBeConsumed = false
        extendsFrom(configurations["implementation"], configurations["runtimeOnly"],
            configurations["testImplementation"], configurations["testRuntimeOnly"], boot41Test)
    }
    // Boot 4.1 manages JUnit 6: the Boot 4.1 run stays on the repository's JUnit, as the Boot 3.5 one
    testBoot4RuntimeClasspath.resolutionStrategy.eachDependency {
        if (requested.group == "org.junit.jupiter") useVersion("5.12.2")
        if (requested.group == "org.junit.platform") useVersion("1.12.2")
    }
    // The tests log through slf4j-simple (CapturedStderr reads it); with Logback present too, SpringApplication's
    // logging system refuses to start ("LoggerFactory is not a Logback LoggerContext")
    listOf(configurations["testRuntimeClasspath"], testBoot4RuntimeClasspath).forEach {
        it.exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
    }

    dependencies {
        "compileOnly"(bootPlatform("3.5"))
        "annotationProcessor"(bootPlatform("3.5"))
        "annotationProcessor"("org.springframework.boot:spring-boot-configuration-processor")

        "testImplementation"(testFixtures(project(":outbox-core")))
        "testImplementation"("org.springframework.boot:spring-boot-starter-jdbc")
        "testImplementation"("org.springframework.boot:spring-boot-starter-data-jpa")
        "testImplementation"("org.springframework.boot:spring-boot-starter-actuator")
        "testImplementation"("org.springframework.boot:spring-boot-starter-test")
        "testImplementation"("org.flywaydb:flyway-database-postgresql")
        "testImplementation"("io.micrometer:micrometer-tracing-bridge-otel")
        "testImplementation"("io.opentelemetry:opentelemetry-sdk-testing")
        "testImplementation"("com.google.protobuf:protobuf-java:3.25.8")

        "boot35Test"(bootPlatform("3.5"))
        "boot35Test"("org.springframework.boot:spring-boot-starter-web")
        "boot35Test"("org.springframework.boot:spring-boot-starter-aop")
        "boot35Test"("org.flywaydb:flyway-core")
        "boot35Test"("org.liquibase:liquibase-core")
        "boot35Test"("tools.jackson.core:jackson-databind:3.1.7")

        // platform, not enforced: the repository's newer Testcontainers and PostgreSQL driver win
        "boot41Test"(bootPlatform("4.1"))
        "boot41Test"("org.springframework.boot:spring-boot-starter-webmvc")
        "boot41Test"("org.springframework.boot:spring-boot-starter-aspectj")
        "boot41Test"("org.springframework.boot:spring-boot-starter-flyway")
        "boot41Test"("org.springframework.boot:spring-boot-starter-liquibase")
        "boot41Test"("org.springframework.boot:spring-boot-starter-jackson")
        "boot41Test"("org.springframework.boot:spring-boot-jackson2")
        // not in Boot 4.1's actuator starter: the tracing observation handlers Boot 3.5 has in its actuator-autoconfigure
        "boot41Test"("org.springframework.boot:spring-boot-micrometer-tracing")
    }

    // Incremental recompiles make the configuration processor lose the properties' descriptions in its metadata
    tasks.withType<JavaCompile>().configureEach { options.isIncremental = false }

    val sourceSets = the<SourceSetContainer>()
    tasks.named<Test>("test") { systemProperty("outbox.test.boot", "3.5") }
    val testBoot4 = tasks.register<Test>("testBoot4") {
        description = "Runs the tests on Spring Boot ${bootLines["4.1"]}"
        group = "verification"
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["main"].output + sourceSets["test"].output + testBoot4RuntimeClasspath
        systemProperty("outbox.test.boot", "4.1")
        shouldRunAfter("test")
    }
    tasks.named("check") { dependsOn(testBoot4) }
}

// Fails when a framework reaches a framework-free module's compile or runtime classpath
mapOf(
    "outbox-core" to listOf("ru.tinkoff.kora", "org.springframework", "io.micrometer", "io.opentelemetry"),
    "outbox-kafka" to listOf("ru.tinkoff.kora", "org.springframework"),
    "outbox-micrometer" to listOf("ru.tinkoff.kora", "org.springframework"),
).forEach { (name, banned) ->
    project(":$name") {
        val classpaths = listOf("compileClasspath", "runtimeClasspath").map { configurations[it] }
        val verifyFrameworkFree by tasks.registering {
            description = "Fails when ${banned.joinToString()} is on the compile or runtime classpath"
            doLast {
                classpaths.forEach { classpath ->
                    val found = classpath.incoming.resolutionResult.allComponents.mapNotNull { it.moduleVersion }
                        .filter { module -> banned.any { module.group.startsWith(it) } }
                    if (found.isNotEmpty()) {
                        throw GradleException("$path ${classpath.name} must stay framework-free: ${found.joinToString()}")
                    }
                }
            }
        }
        tasks.named("check") { dependsOn(verifyFrameworkFree) }
    }
}
