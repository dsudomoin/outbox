package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HashingTest {

    // Frozen: shards of rows already written depend on these values. Never change them.
    @Test
    void goldenValues() {
        int keyed = Hashing.keyHash("orders", "user-1".getBytes(UTF_8));
        assertThat(keyed).isEqualTo(-1529465446);
        assertThat(Hashing.keyHash("orders", null)).isEqualTo(1244240942);
        assertThat(Hashing.shard(keyed, 32)).isEqualTo(26);
    }

    @Test
    void randomShardStaysInRange() {
        for (int i = 0; i < 1000; i++) {
            assertThat(Hashing.randomShard(32)).isBetween(0, 31);
        }
    }
}
