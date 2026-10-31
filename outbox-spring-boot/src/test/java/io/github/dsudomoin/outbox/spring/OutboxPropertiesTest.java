package io.github.dsudomoin.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxConfig;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class OutboxPropertiesTest {

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OutboxProperties.class)
    static class PropertiesConfig {}

    private static void bind(Consumer<OutboxProperties> check, String... properties) {
        new ApplicationContextRunner().withUserConfiguration(PropertiesConfig.class).withPropertyValues(properties)
            .run(context -> {
                assertThat(context).hasNotFailed();
                check.accept(context.getBean(OutboxProperties.class));
            });
    }

    @Test
    void runsOnTheBootOfItsTestTask() {
        assertThat(SpringBootVersion.getVersion()).startsWith(System.getProperty("outbox.test.boot") + ".");
    }

    @Test
    void emptyEnvironmentEqualsCoreDefaults() {
        bind(bound -> {
            List<String> compared = new ArrayList<>();
            assertSameValues("outbox", OutboxConfig.class, new OutboxConfig() {}, bound, compared);
            assertThat(compared).contains("outbox.partitions.tail-conversion.zone", "outbox.relay.retry.max-attempts");
        });
    }

    /** Compares every setting accessor of {@code type}, descending into the nested {@code *Config} interfaces. */
    private static void assertSameValues(String path, Class<?> type, Object expected, Object actual,
                                         List<String> compared) {
        for (Method accessor : settingAccessors(type)) {
            String key = path + "." + kebab(accessor.getName());
            Object want = invoke(accessor, expected);
            Object got = invoke(accessor, actual);
            if (isConfigInterface(accessor.getReturnType())) {
                assertSameValues(key, accessor.getReturnType(), want, got, compared);
            } else {
                assertThat(got).as(key).isEqualTo(want);
                compared.add(key);
            }
        }
    }

    @Test
    void kebabAndCamelKeysBind() {
        bind(p -> assertThat(p.relay().batchSize()).isEqualTo(7), "outbox.relay.batch-size=7");
        bind(p -> assertThat(p.relay().batchSize()).isEqualTo(7), "outbox.relay.batchSize=7");
        bind(p -> assertThat(p.deadLetters().batchSize()).isEqualTo(5), "outbox.dead-letters.batch-size=5");
        bind(p -> assertThat(p.partitions().tailConversion().window()).isEmpty(),
            "outbox.partitions.tail-conversion.window=");
        bind(p -> assertThat(p.shards()).isEqualTo(16), "outbox.shards=16");
    }

    @Test
    void springOnlyDefaults() {
        bind(p -> {
            assertThat(p.enabled()).isTrue();
            assertThat(p.lifecycle().phase()).isEqualTo(Integer.MAX_VALUE - 4096);
            assertThat(p.telemetry().observeUnsampled()).isFalse();
            assertThat(p.json().mapper()).isEqualTo(OutboxProperties.Json.Mapper.AUTO);
            assertThat(p.heartbeat().dedicatedConnection()).isTrue();
        });
        bind(p -> assertThat(p.json().mapper()).isEqualTo(OutboxProperties.Json.Mapper.JACKSON3),
            "outbox.json.mapper=jackson3");
        bind(p -> assertThat(p.heartbeat().dedicatedConnection()).isFalse(),
            "outbox.heartbeat.dedicated-connection=false");
        bind(p -> assertThat(p.heartbeat().dedicatedConnection()).isFalse(),
            "outbox.heartbeat.dedicatedConnection=false");
    }

    @Test
    @SuppressWarnings("unchecked")
    void metadataHasEveryKeyWithDefault() throws Exception {
        Map<String, Map<String, Object>> properties = new HashMap<>();
        for (URL url : Collections.list(getClass().getClassLoader()
                .getResources("META-INF/spring-configuration-metadata.json"))) {
            try (InputStream in = url.openStream()) {
                Map<String, Object> metadata = JsonParserFactory.getJsonParser()
                    .parseMap(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                for (Object property : (List<Object>) metadata.getOrDefault("properties", List.of())) {
                    Map<String, Object> p = (Map<String, Object>) property;
                    properties.put((String) p.get("name"), p);
                }
            }
        }
        List<String> keys = new ArrayList<>();
        collectKeys("outbox", OutboxConfig.class, keys);
        assertThat(keys).hasSizeGreaterThan(40);
        for (String key : keys) {
            assertThat(properties).as("metadata of " + key).containsKey(key);
            assertThat((String) properties.get(key).get("description")).as("description of " + key).isNotBlank();
        }
        assertThat(String.valueOf(properties.get("outbox.relay.batch-size").get("defaultValue"))).isEqualTo("1000");
        assertThat(properties.get("outbox.relay.batch-timeout").get("defaultValue")).isEqualTo("30s");
        assertThat(String.valueOf(properties.get("outbox.heartbeat.dedicated-connection").get("defaultValue")))
            .isEqualTo("true");
        assertThat((String) properties.get("outbox.heartbeat.dedicated-connection").get("description")).isNotBlank();
    }

    private static void collectKeys(String path, Class<?> type, List<String> keys) {
        for (Method accessor : settingAccessors(type)) {
            String key = path + "." + kebab(accessor.getName());
            if (isConfigInterface(accessor.getReturnType())) {
                collectKeys(key, accessor.getReturnType(), keys);
            } else {
                keys.add(key);
            }
        }
    }

    @Test
    void malformedDurationNamesTheKey() {
        new ApplicationContextRunner().withUserConfiguration(PropertiesConfig.class)
            .withPropertyValues("outbox.relay.batch-timeout=30 seconds")
            .run(context -> {
                assertThat(context).hasFailed();
                List<String> messages = new ArrayList<>();
                for (Throwable t = context.getStartupFailure(); t != null; t = t.getCause()) {
                    messages.add(String.valueOf(t.getMessage()));
                }
                assertThat(String.join("\n", messages)).contains("outbox.relay.batch-timeout");
            });
    }

    private static List<Method> settingAccessors(Class<?> type) {
        List<Method> accessors = new ArrayList<>();
        for (Method m : type.getMethods()) {
            if (!Modifier.isStatic(m.getModifiers()) && m.getParameterCount() == 0) {
                accessors.add(m);
            }
        }
        return accessors;
    }

    private static boolean isConfigInterface(Class<?> type) {
        return type.isInterface() && type.getEnclosingClass() == OutboxConfig.class;
    }

    private static Object invoke(Method accessor, Object target) {
        try {
            return accessor.invoke(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static String kebab(String camel) {
        return camel.replaceAll("([A-Z])", "-$1").toLowerCase();
    }
}
