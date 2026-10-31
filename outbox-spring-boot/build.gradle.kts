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
    compileOnly("com.fasterxml.jackson.core:jackson-databind")
    compileOnly("tools.jackson.core:jackson-databind:3.1.7")
    compileOnly("com.google.protobuf:protobuf-java:3.25.8")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")
}
