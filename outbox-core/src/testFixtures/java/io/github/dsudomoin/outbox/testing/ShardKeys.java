package io.github.dsudomoin.outbox.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.internal.Hashing;
import java.util.HashMap;
import java.util.Map;

/** Keys of {@code topic} that land on a given shard, a new one each time: {@code <shard>-<n>}. */
public final class ShardKeys {

    private final String topic;
    private final int shards;
    private final Map<Integer, Integer> next = new HashMap<>();

    /** The first key {@code <prefix><i>} of {@code topic} on {@code shard}: the same one each time. */
    public static String first(String topic, String prefix, int shard, int shards) {
        for (int i = 0; ; i++) {
            var key = prefix + i;
            if (Hashing.shard(Hashing.keyHash(topic, key.getBytes(UTF_8)), shards) == shard) {
                return key;
            }
        }
    }

    public ShardKeys(String topic, int shards) {
        this.topic = topic;
        this.shards = shards;
    }

    /** The next key of {@code shard}. */
    public synchronized String next(int shard) {
        int i = next.getOrDefault(shard, 0);
        while (true) {
            var key = shard + "-" + i++;
            if (Hashing.shard(Hashing.keyHash(topic, key.getBytes(UTF_8)), shards) == shard) {
                next.put(shard, i);
                return key;
            }
        }
    }
}
