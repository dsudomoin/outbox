package io.github.dsudomoin.outbox.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Keeps the docs honest: every Java block under the {@code ## Spring Boot} heading of README.md and of each skill
 * compiles against the running Boot line, and the README's first screen starts as it is written. The blocks of a
 * document share a package, so they may use each other's types, and the imports below, which the docs leave out.
 */
class ReadmeExamplesTest {

    private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();
    private static final String HEADING = "## Spring Boot\n";
    private static final Pattern NEXT_SECTION = Pattern.compile("^## ", Pattern.MULTILINE);
    private static final Pattern JAVA_BLOCK = Pattern.compile("^ *```java\n(.*?)^ *```$",
        Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern YAML_BLOCK = Pattern.compile("^```yaml\n(.*?)^```$",
        Pattern.MULTILINE | Pattern.DOTALL);
    private static final String IMPORTS = """
        package readme;

        import static java.nio.charset.StandardCharsets.UTF_8;
        import static org.assertj.core.api.Assertions.assertThat;

        import com.zaxxer.hikari.HikariDataSource;
        import io.github.dsudomoin.outbox.*;
        import io.github.dsudomoin.outbox.admin.spring.*;
        import io.github.dsudomoin.outbox.kafka.*;
        import io.github.dsudomoin.outbox.spring.*;
        import jakarta.servlet.http.HttpServletRequest;
        import java.io.File;
import java.io.IOException;
        import java.nio.file.Files;
        import java.nio.file.Path;
        import java.security.MessageDigest;
        import java.util.*;
        import javax.sql.DataSource;
        import org.apache.kafka.clients.consumer.ConsumerRecord;
        import org.junit.jupiter.api.Test;
        import org.springframework.beans.factory.ObjectProvider;
        import org.springframework.beans.factory.annotation.Autowired;
        import org.springframework.beans.factory.annotation.Value;
        import org.springframework.boot.jdbc.DataSourceBuilder;
        import org.springframework.boot.test.context.SpringBootTest;
        import org.springframework.context.annotation.Bean;
        import org.springframework.context.annotation.Configuration;
        import org.springframework.jdbc.core.JdbcTemplate;
        import org.springframework.stereotype.Component;
        import org.springframework.stereotype.Service;
        import org.springframework.test.context.DynamicPropertyRegistry;
        import org.springframework.test.context.DynamicPropertySource;
        import org.springframework.transaction.PlatformTransactionManager;
        import org.springframework.transaction.annotation.Transactional;
        import org.springframework.transaction.support.TransactionTemplate;
        import org.testcontainers.postgresql.PostgreSQLContainer;

        """;

    /** README.md and every skill with a Spring Boot section. */
    static List<String> documents() throws IOException {
        var documents = new ArrayList<>(List.of("README.md"));
        try (Stream<Path> skills = Files.walk(REPOSITORY.resolve("skills"))) {
            skills.filter(path -> path.toString().endsWith(".md")).filter(ReadmeExamplesTest::hasSection)
                .map(path -> REPOSITORY.relativize(path).toString()).forEach(documents::add);
        }
        return documents;
    }

    private static boolean hasSection(Path document) {
        try {
            return Files.readString(document).contains("\n" + HEADING);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Fails the test when a skill loses its section, which would quietly leave its snippets unchecked. */
    @Test
    void theDocsHaveTheirSpringBootSections() throws IOException {
        assertThat(documents()).contains("README.md", "skills/using-outbox/SKILL.md",
            "skills/using-outbox/config-reference.md", "skills/writing-outbox-handlers/SKILL.md",
            "skills/testing-outbox/SKILL.md", "skills/operating-outbox/SKILL.md",
            "skills/operating-outbox/admin-api.md", "skills/consuming-with-outbox-inbox/SKILL.md");
        assertThat(JAVA_BLOCK.matcher(section("README.md")).results().count()).isGreaterThan(10);
    }

    @ParameterizedTest
    @MethodSource("documents")
    void springBootSnippetsCompile(String document, @TempDir Path directory) throws IOException {
        compile(document, directory);
    }

    /** Boot's own DataSource, Kafka and the admin API, as the first screen writes them; the relay stays off. */
    @Test
    void theFirstScreenStarts(@TempDir Path directory) throws Exception {
        var classes = compile("README.md", directory);
        var yaml = YAML_BLOCK.matcher(section("README.md")).results().findFirst().orElseThrow().group(1);
        var properties = new YamlPropertySourceLoader().load("README.md", new ByteArrayResource(yaml.getBytes(UTF_8)));
        TestApps.execute("create table if not exists orders (id uuid primary key, user_id text)");

        try (var loader = new URLClassLoader(urls(classes), getClass().getClassLoader())) {
            var orders = loader.loadClass("readme.Orders");
            new WebApplicationContextRunner().withClassLoader(loader)
                .withInitializer(context -> properties.forEach(context.getEnvironment().getPropertySources()::addLast))
                .withUserConfiguration(TestApps.Boot.class, orders, loader.loadClass("readme.ApiKeyAuthorizer"))
                .withPropertyValues(TestApps.properties("spring.kafka.bootstrap-servers=localhost:9092",
                    "outbox.relay.enabled=false", "outbox-admin.admin-key=a", "outbox-admin.reader-key=r"))
                .run(context -> {
                    assertThat(context).hasNotFailed().hasBean("outboxKafkaFallbackHandler")
                        .hasBean("outboxAdminController");
                    var id = UUID.randomUUID();

                    var message = ReflectionTestUtils.invokeMethod(context.getBean(orders), "create", id, "u1");

                    assertThat(TestApps.queryLong("select count(*) from orders where id = '" + id + "'")).isOne();
                    assertThat(TestApps.queryLong("select count(*) from " + TestApps.table(context)
                        + " where message_id = '" + message + "'")).isOne();
                });
        }
    }

    private static String section(String document) throws IOException {
        var text = Files.readString(REPOSITORY.resolve(document));
        var start = text.indexOf("\n" + HEADING);
        assertThat(start).as(document + " has a Spring Boot section").isNotNegative();
        var body = text.substring(start + HEADING.length() + 1);
        var next = NEXT_SECTION.matcher(body);
        return next.find() ? body.substring(0, next.start()) : body;
    }

    /** The document's blocks, compiled into a directory it returns; a block that does not compile fails the test. */
    private static Path compile(String document, Path directory) throws IOException {
        var sources = new ArrayList<Path>();
        for (var block : JAVA_BLOCK.matcher(section(document)).results().toList()) {
            var source = directory.resolve("Block" + sources.size() + ".java");
            Files.writeString(source, IMPORTS + block.group(1));
            sources.add(source);
        }
        var classes = Files.createDirectory(directory.resolve("classes"));
        if (sources.isEmpty()) {
            return classes;
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, null, UTF_8)) {
            var options = List.of("-proc:none", "-d", classes.toString(), "-cp",
                System.getProperty("java.class.path") + File.pathSeparator + extraClasspath());
            boolean compiled = compiler.getTask(null, files, diagnostics, options, null,
                files.getJavaFileObjectsFromPaths(sources)).call();
            assertThat(compiled).as(() -> document + ":\n" + diagnostics.getDiagnostics()).isTrue();
        }
        return classes;
    }

    private static String extraClasspath() {
        return System.getProperty("outbox.test.readme.classpath");
    }

    private static URL[] urls(Path classes) throws MalformedURLException {
        var urls = new ArrayList<>(List.of(classes.toUri().toURL()));
        for (var path : extraClasspath().split(File.pathSeparator)) {
            urls.add(Path.of(path).toUri().toURL());
        }
        return urls.toArray(URL[]::new);
    }
}
