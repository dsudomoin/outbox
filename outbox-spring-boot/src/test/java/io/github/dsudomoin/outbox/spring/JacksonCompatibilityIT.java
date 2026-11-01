package io.github.dsudomoin.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.exc.InvalidFormatException;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;

/**
 * What a payload does when a service moves between Boot's configured Jackson 2 {@code ObjectMapper} and Jackson 3
 * {@code JsonMapper}, on Boot 4.1, which has both: same JSON and values unless a test here says otherwise.
 */
@EnabledIfSystemProperty(named = "outbox.test.boot", matches = "4\\.1")
class JacksonCompatibilityIT {

    private static ObjectMapper jackson2;
    private static JsonMapper jackson3;

    public enum Status { ACTIVE, CLOSED }

    /** An enum with its own {@code toString}. */
    public enum Labelled {
        ACTIVE;

        @Override
        public String toString() {
            return "active-label";
        }
    }

    public record Payload(Instant instant, LocalDate date, Duration duration, Status status, BigDecimal amount,
        String note) {}

    public record Primitive(long id, String note) {}

    public record WithLabel(Labelled label) {}

    public record Amount(BigDecimal amount) {}

    /** Not a record: its properties have no creator order. */
    public static class Bean {

        public String getZ() {
            return "z";
        }

        public String getA() {
            return "a";
        }
    }

    private static final Payload PAYLOAD = new Payload(Instant.parse("2026-11-02T10:15:30.123456789Z"),
        LocalDate.of(2026, 11, 2), Duration.ofSeconds(90, 5), Status.ACTIVE, new BigDecimal("12.50"), null);
    private static final String PAYLOAD_JSON = "{\"instant\":\"2026-11-02T10:15:30.123456789Z\","
        + "\"date\":\"2026-11-02\",\"duration\":\"PT1M30.000000005S\",\"status\":\"ACTIVE\",\"amount\":12.50,\"note\":null}";

    @BeforeAll
    static void bootsMappers() throws Exception {
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
                Class.forName("org.springframework.boot.jackson2.autoconfigure.Jackson2AutoConfiguration"),
                Class.forName("org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration")))
            .run(context -> {
                jackson2 = context.getBean(ObjectMapper.class);
                jackson3 = context.getBean(JsonMapper.class);
            });
    }

    @Test
    void datesEnumDecimalsAndNullsWriteTheSameJson() throws Exception {
        assertThat(jackson2.writeValueAsString(PAYLOAD)).isEqualTo(PAYLOAD_JSON);
        assertThat(jackson3.writeValueAsString(PAYLOAD)).isEqualTo(PAYLOAD_JSON);
    }

    @Test
    void datesEnumDecimalsAndNullsReadBackEqual() throws Exception {
        assertThat(jackson3.readValue(jackson2.writeValueAsBytes(PAYLOAD), Payload.class)).isEqualTo(PAYLOAD);
        assertThat(jackson2.readValue(jackson3.writeValueAsBytes(PAYLOAD), Payload.class)).isEqualTo(PAYLOAD);
    }

    /** An old payload written as numeric timestamps still reads on both. */
    @Test
    void numericInstantReadsOnBoth() throws Exception {
        var json = "{\"instant\":1793614530.123456789}";
        var expected = new Payload(Instant.parse("2026-11-02T10:15:30.123456789Z"), null, null, null, null, null);

        assertThat(jackson2.readValue(json, Payload.class)).isEqualTo(expected);
        assertThat(jackson3.readValue(json, Payload.class)).isEqualTo(expected);
    }

    @Test
    void decimalsKeepScaleAndExponent() throws Exception {
        for (var text : new String[] {"1.10", "1E+3", "12345678901234567890.123456789"}) {
            var amount = new Amount(new BigDecimal(text));
            var json = "{\"amount\":" + text + "}";

            assertThat(jackson2.writeValueAsString(amount)).isEqualTo(json);
            assertThat(jackson3.writeValueAsString(amount)).isEqualTo(json);
            assertThat(jackson2.readValue(json, Amount.class)).isEqualTo(amount);
            assertThat(jackson3.readValue(json, Amount.class)).isEqualTo(amount);
        }
    }

    @Test
    void missingReferenceFieldReadsAsNullOnBoth() throws Exception {
        var json = "{\"id\":1}";

        assertThat(jackson2.readValue(json, Primitive.class)).isEqualTo(new Primitive(1, null));
        assertThat(jackson3.readValue(json, Primitive.class)).isEqualTo(new Primitive(1, null));
    }

    @Test
    void unknownFieldIsIgnoredByBoth() throws Exception {
        var json = "{\"id\":1,\"note\":\"n\",\"added\":{\"later\":true}}";

        assertThat(jackson2.readValue(json, Primitive.class)).isEqualTo(new Primitive(1, "n"));
        assertThat(jackson3.readValue(json, Primitive.class)).isEqualTo(new Primitive(1, "n"));
    }

    /** Difference: Jackson 2 reads 0, Jackson 3 refuses (FAIL_ON_NULL_FOR_PRIMITIVES is on by default). */
    @Test
    void missingPrimitiveFieldIsZeroOnJackson2AndFailsOnJackson3() throws Exception {
        var json = "{\"note\":\"n\"}";

        assertThat(jackson2.readValue(json, Primitive.class)).isEqualTo(new Primitive(0, "n"));
        assertThatThrownBy(() -> jackson3.readValue(json, Primitive.class))
            .isInstanceOf(MismatchedInputException.class).hasMessageContaining("Cannot map `null` into type `long`");
    }

    /** Difference: as a missing one. */
    @Test
    void nullPrimitiveFieldIsZeroOnJackson2AndFailsOnJackson3() throws Exception {
        var json = "{\"id\":null}";

        assertThat(jackson2.readValue(json, Primitive.class)).isEqualTo(new Primitive(0, null));
        assertThatThrownBy(() -> jackson3.readValue(json, Primitive.class))
            .isInstanceOf(MismatchedInputException.class).hasMessageContaining("Cannot map `null` into type `long`");
    }

    /**
     * Difference: Jackson 2 writes and reads an enum's {@code name()}, Jackson 3 its {@code toString()}
     * (EnumFeature.WRITE_ENUMS_USING_TO_STRING and READ_ENUMS_USING_TO_STRING), so with an own {@code toString} neither
     * reads what the other wrote.
     */
    @Test
    void enumWithOwnToStringIsNotReadableAcrossMajors() throws Exception {
        var value = new WithLabel(Labelled.ACTIVE);
        var byJackson2 = jackson2.writeValueAsString(value);
        var byJackson3 = jackson3.writeValueAsString(value);

        assertThat(byJackson2).isEqualTo("{\"label\":\"ACTIVE\"}");
        assertThat(byJackson3).isEqualTo("{\"label\":\"active-label\"}");
        assertThat(jackson2.readValue(byJackson2, WithLabel.class)).isEqualTo(value);
        assertThat(jackson3.readValue(byJackson3, WithLabel.class)).isEqualTo(value);
        assertThatThrownBy(() -> jackson3.readValue(byJackson2, WithLabel.class))
            .isInstanceOf(InvalidFormatException.class).hasMessageContaining("from String \"ACTIVE\"");
        assertThatThrownBy(() -> jackson2.readValue(byJackson3, WithLabel.class))
            .isInstanceOf(com.fasterxml.jackson.databind.exc.InvalidFormatException.class)
            .hasMessageContaining("from String \"active-label\"");
    }

    /** Difference: a class's properties in Jackson 3 are sorted alphabetically; records keep their order on both. */
    @Test
    void beanPropertyOrder() throws Exception {
        assertThat(jackson2.writeValueAsString(new Bean())).isEqualTo("{\"z\":\"z\",\"a\":\"a\"}");
        assertThat(jackson3.writeValueAsString(new Bean())).isEqualTo("{\"a\":\"a\",\"z\":\"z\"}");
    }
}
