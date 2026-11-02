// java-library, publishing, the Boot 3.5 compile platform and both test runs (test on Boot 3.5, testBoot4 on Boot 4.1)
// are configured by the root build. Spring and Boot are compileOnly: the application brings its Boot.
dependencies {
    api(project(":outbox-core"))
    api(project(":outbox-micrometer"))
    implementation("org.slf4j:slf4j-api:2.0.17")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework:spring-jdbc")
    compileOnly("org.springframework:spring-tx")
    compileOnly("org.springframework:spring-aop")
    // optional: the app's classpath decides which of them are used
    compileOnly("io.micrometer:micrometer-tracing")
    compileOnly("com.zaxxer:HikariCP")
    compileOnly("com.fasterxml.jackson.core:jackson-databind")
    compileOnly("tools.jackson.core:jackson-databind:3.1.7")
    compileOnly("com.google.protobuf:protobuf-java:3.25.8")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")
}

// ReadmeExamplesTest compiles the docs' snippets against these and starts the smallest app with them. Not on the test
// classpath: there they would auto-configure into every other test of this module.
val readmeExamples by configurations.creating {
    isCanBeConsumed = false
}

dependencies {
    readmeExamples(project(":outbox-kafka-spring-boot"))
    readmeExamples(project(":outbox-admin-spring-boot"))
}

tasks.withType<Test>().configureEach {
    inputs.files(readmeExamples).withNormalizer(ClasspathNormalizer::class)
    inputs.files(rootProject.file("README.md"), rootProject.fileTree("skills"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf("-Doutbox.test.readme.classpath=${readmeExamples.asPath}")
    })
}
