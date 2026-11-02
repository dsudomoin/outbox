// java-library, publishing, the Boot 3.5 compile platform and both test runs (test on Boot 3.5, testBoot4 on Boot 4.1)
// are configured by the root build. Spring and Boot are compileOnly: the application brings its Boot.
plugins {
    id("org.openapi.generator") version "7.14.0"
}

// The one contract, outbox-admin-kora's. Interfaces and plain models only, no base-path property; absent containers
// stay null, as the Selection's "exactly one of" and the optional response fields need; date-times are strings, written
// and parsed here, and absent fields are left out per model, so that the app's mapper settings cannot change them.
openApiGenerate {
    generatorName = "spring"
    inputSpec = rootProject.file("outbox-admin-kora/src/main/resources/openapi/kora-outbox-admin.yaml").path
    outputDir = layout.buildDirectory.dir("generated/openapi").get().asFile.path
    apiPackage = "io.github.dsudomoin.outbox.admin.spring.api"
    modelPackage = "io.github.dsudomoin.outbox.admin.spring.model"
    globalProperties = mapOf("apis" to "", "models" to "")
    typeMappings = mapOf("OffsetDateTime" to "String")
    configOptions = mapOf(
        "sourceFolder" to "",
        "interfaceOnly" to "true",
        "skipDefaultInterface" to "true",
        "useTags" to "true",
        "useSpringBoot3" to "true",
        "useBeanValidation" to "false",
        "openApiNullable" to "false",
        "documentationProvider" to "none",
        "annotationLibrary" to "none",
        "requestMappingMode" to "none",
        "containerDefaultToNull" to "true",
        "hideGenerationTimestamp" to "true",
        "additionalModelTypeAnnotations" to "@com.fasterxml.jackson.annotation.JsonInclude(" +
            "com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)",
    )
}

sourceSets.main {
    java.srcDir(tasks.openApiGenerate.map { it.outputDir }) // compileJava and sourcesJar depend on the generation
}

tasks.sourcesJar {
    exclude(".openapi-generator*", ".openapi-generator/**") // the generator's bookkeeping, not sources
}

dependencies {
    api(project(":outbox-spring-boot"))
    implementation("org.slf4j:slf4j-api:2.0.17")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework:spring-webmvc")
    compileOnly("jakarta.servlet:jakarta.servlet-api")
    compileOnly("com.fasterxml.jackson.core:jackson-annotations")
    // optional: Jackson3Tree and Jackson2Tree read the bodies, by the major the app's classpath has
    compileOnly("com.fasterxml.jackson.core:jackson-databind")
    compileOnly("tools.jackson.core:jackson-databind:3.1.7")
    compileOnly("jakarta.annotation:jakarta.annotation-api:2.1.1")
    // an import and @NotNull the generator writes into the models whatever useBeanValidation says; nothing validates
    compileOnly("jakarta.validation:jakarta.validation-api")
}
