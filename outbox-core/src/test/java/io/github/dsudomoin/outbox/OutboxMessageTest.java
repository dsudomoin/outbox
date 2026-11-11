package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutboxMessageTest {

    @Test
    void topicLongerThan249Rejected() {
        assertThatCode(() -> OutboxMessage.of("t".repeat(249), null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> OutboxMessage.of("t".repeat(250), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keyLongerThan1024BytesRejected() {
        assertThatCode(() -> OutboxMessage.of("t", new byte[1024], null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> OutboxMessage.of("t", new byte[1025], null))
                .isInstanceOf(IllegalArgumentException.class);
        // the limit counts UTF-8 bytes, not chars: 513 two-byte chars = 1026 bytes
        assertThatThrownBy(() -> OutboxMessage.of("t", "я".repeat(513), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topicPostgresCannotStoreRejected() {
        // NUL does not fit Postgres text; an unpaired surrogate would be stored as '?' under another name
        for (var topic : List.of("t\u0000x", "a\uD800b", "a\uDC00", "\uDC00a", "tenant-\uD83D", "a\uD800\uD800b")) {
            assertThatThrownBy(() -> OutboxMessage.of(topic, null))
                    .as(topic)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new OutboxMessage("a\uD800b", null, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> OutboxMessage.of("t\uD83D\uDE00", null)).doesNotThrowAnyException();
        assertThatCode(() -> OutboxMessage.of("t", null).withHeader("h\u0000", null)).doesNotThrowAnyException();
    }

    @Test
    void blankTopicRejected() {
        assertThatThrownBy(() -> OutboxMessage.of("", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutboxMessage.of(" \t", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutboxMessage.of(null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unkeyedFactoryHasNullKey() {
        byte[] payload = "p".getBytes(UTF_8);
        var message = OutboxMessage.of("t", payload);
        assertThat(message.topic()).isEqualTo("t");
        assertThat(message.key()).isNull();
        assertThat(message.payload()).isSameAs(payload);
        assertThat(message.headers()).isEmpty();
    }

    @Test
    void stringKeyIsUtf8() {
        assertThat(OutboxMessage.of("t", "ключ-1", null).key()).isEqualTo("ключ-1".getBytes(UTF_8));
        assertThat(OutboxMessage.of("t", (String) null, null).key()).isNull();
    }

    @Test
    void headersAreCopiedAndOrdered() {
        var b1 = new OutboxHeader("b", "1".getBytes(UTF_8));
        var a2 = new OutboxHeader("a", "2".getBytes(UTF_8));
        var b3 = new OutboxHeader("b", null);
        var source = new ArrayList<>(List.of(b1, a2, b3));

        var message = new OutboxMessage("t", null, null, source);
        source.clear();

        assertThat(message.headers()).containsExactly(b1, a2, b3);
        assertThatThrownBy(() -> message.headers().add(b1)).isInstanceOf(UnsupportedOperationException.class);

        var withC = message.withHeader("c", "4".getBytes(UTF_8));
        assertThat(withC.headers()).containsExactly(b1, a2, b3, new OutboxHeader("c", "4".getBytes(UTF_8)));
        assertThat(message.headers()).containsExactly(b1, a2, b3);
    }

    @Test
    void nullPayloadAllowed() {
        var message = OutboxMessage.of("t", "k", null);
        assertThat(message.payload()).isNull();
        assertThat(message.headers()).isEmpty();
    }
}
