package prodcheck.target;

import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import ru.tinkoff.kora.common.Component;

/** What {@code PUT /fault} set: how the target misbehaves. */
@Component
public final class Fault {

    volatile String mode = "ok";
    volatile long delayMs;
    volatile Set<String> failKeys = Set.of();
    volatile double failPercent;
    volatile double dropPercent;
    volatile boolean poisonFixed;
    volatile boolean countDoubles;

    /** Omitted parameters keep their value; {@code failKeys=} clears. */
    void update(@Nullable String mode, @Nullable String delayMs, @Nullable String failKeys,
            @Nullable String failPercent, @Nullable String dropPercent, @Nullable String poisonFixed,
            @Nullable String countDoubles) {
        if (mode != null && !Set.of("ok", "error", "hang").contains(mode)) {
            throw new IllegalArgumentException("mode must be ok, error or hang, got " + mode);
        }
        var delay = delayMs == null ? null : Long.valueOf(delayMs);
        var fail = failPercent == null ? null : Double.valueOf(failPercent);
        var drop = dropPercent == null ? null : Double.valueOf(dropPercent);
        if (delay != null) {
            this.delayMs = delay;
        }
        if (failKeys != null) {
            this.failKeys = Arrays.stream(failKeys.split(",")).filter(k -> !k.isBlank())
                    .collect(Collectors.toUnmodifiableSet());
        }
        if (fail != null) {
            this.failPercent = fail;
        }
        if (drop != null) {
            this.dropPercent = drop;
        }
        if (poisonFixed != null) {
            this.poisonFixed = Boolean.parseBoolean(poisonFixed);
        }
        if (countDoubles != null) {
            this.countDoubles = Boolean.parseBoolean(countDoubles);
        }
        if (mode != null) {
            this.mode = mode;
        }
    }

    Map<String, Object> asMap() {
        var map = new LinkedHashMap<String, Object>();
        map.put("mode", mode);
        map.put("delayMs", delayMs);
        map.put("failKeys", failKeys);
        map.put("failPercent", failPercent);
        map.put("dropPercent", dropPercent);
        map.put("poisonFixed", poisonFixed);
        map.put("countDoubles", countDoubles);
        return map;
    }
}
