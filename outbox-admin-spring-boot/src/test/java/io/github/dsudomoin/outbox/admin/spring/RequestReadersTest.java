package io.github.dsudomoin.outbox.admin.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** The two trees alike, and {@link RequestReaders} on a classpath that has one Jackson, as an app's has, or none. */
class RequestReadersTest {

    private static final String JACKSON2 = "com.fasterxml.jackson.core" + File.separator + "jackson-databind";
    private static final String JACKSON3 = "tools.jackson.core";

    private static List<Function<byte[], Object>> trees() {
        return List.of(Jackson2Tree::read, Jackson3Tree::read);
    }

    @Test
    void bothReadPlainValuesOfACleanMapper() {
        for (var tree : trees()) {
            @SuppressWarnings("unchecked")
            var read = (Map<String, Object>) tree.apply(
                bytes("{\"ids\":[1,5000000000],\"key\":\"  \",\"dry\":true,\"none\":null}"));

            assertThat(read).containsOnlyKeys("ids", "key", "dry", "none").containsEntry("ids", List.of(1, 5_000_000_000L))
                .containsEntry("key", "  ").containsEntry("dry", true).containsEntry("none", null);
            assertThat(tree.apply(bytes("null"))).isNull();
            assertThat(tree.apply(bytes("[]"))).isEqualTo(List.of());
        }
    }

    @Test
    void bothRefuseTrailingTokensAndAnswerInTheParsersWords() {
        for (var tree : trees()) {
            for (var trailing : List.of("{} {}", "{} x", "{}}", "1 2", "null 1")) {
                assertThatThrownBy(() -> tree.apply(bytes(trailing))).as(trailing)
                    .isInstanceOf(IllegalArgumentException.class);
            }
            assertThat(tree.apply(bytes(" {} \n"))).isEqualTo(Map.of());
            assertThatThrownBy(() -> tree.apply(bytes("not json"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unrecognized token 'not'").hasMessageNotContaining("line:");
        }
    }

    @Test
    void readsOnEitherMajorAlone() throws Exception {
        for (var absent : List.of(JACKSON2, JACKSON3)) {
            try (var loader = classpathWithout(absent)) {
                var type = loader.loadClass(RequestReaders.class.getName());
                var constructor = type.getDeclaredConstructor();
                constructor.setAccessible(true);
                var field = type.getDeclaredField("tree");
                field.setAccessible(true);

                @SuppressWarnings("unchecked")
                var tree = (Function<byte[], Object>) field.get(constructor.newInstance());

                assertThat(tree.apply(bytes("{\"topic\":\"t\"}"))).as(absent).isEqualTo(Map.of("topic", "t"));
                assertThatThrownBy(() -> tree.apply(bytes("{} x"))).as(absent)
                    .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    void noJacksonFailsNamingWhatIsMissing() throws Exception {
        try (var loader = classpathWithout(JACKSON2, JACKSON3)) {
            var constructor = loader.loadClass(RequestReaders.class.getName()).getDeclaredConstructor();
            constructor.setAccessible(true);

            assertThatThrownBy(constructor::newInstance).isInstanceOf(InvocationTargetException.class)
                .cause().isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tools.jackson.databind.json.JsonMapper")
                .hasMessageContaining("com.fasterxml.jackson.databind.ObjectMapper");
        }
    }

    /** This test's classpath, less the jars whose path has one of {@code absent}, alone: nothing of the parent's. */
    private static URLClassLoader classpathWithout(String... absent) {
        var urls = new ArrayList<URL>();
        Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(entry -> Arrays.stream(absent).noneMatch(entry::contains))
            .forEach(entry -> {
                try {
                    urls.add(Path.of(entry).toUri().toURL());
                } catch (MalformedURLException e) {
                    throw new AssertionError(e);
                }
            });
        assertThat(urls).as("the classpath is visible").hasSizeGreaterThan(10);
        return new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
    }

    private static byte[] bytes(String json) {
        return json.getBytes(UTF_8);
    }
}
