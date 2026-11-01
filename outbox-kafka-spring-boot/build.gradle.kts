// java-library, publishing, the Boot 3.5 compile platform and both test runs (test on Boot 3.5, testBoot4 on Boot 4.1)
// are configured by the root build. Spring and Boot are compileOnly: the application brings its Boot.
dependencies {
    api(project(":outbox-kafka"))
    api(project(":outbox-spring-boot"))
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")

    testImplementation(testFixtures(project(":outbox-kafka")))
}
