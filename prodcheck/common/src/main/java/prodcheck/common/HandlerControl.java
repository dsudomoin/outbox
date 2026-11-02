package prodcheck.common;

import io.github.dsudomoin.outbox.OutboxPermanentException;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Runtime behaviour of the app's in-process handlers, set by {@code PUT /handler}. */
public final class HandlerControl {

    public static final class State {
        public volatile String mode = "ok";
        public volatile long delayMs;
        public volatile Set<String> failKeys = Set.of();
        public volatile boolean poisonFixed;
        public volatile boolean waiting;
    }

    private static final long ONE_HOUR_NANOS = 3_600_000_000_000L;

    private final Map<String, State> routes = new LinkedHashMap<>();
    private final Set<String> modes = new HashSet<>(Set.of("ok", "fail", "slow", "hang", "hang-hard"));

    public HandlerControl(List<String> routes, Set<String> extraModes) {
        routes.forEach(route -> this.routes.put(route, new State()));
        modes.addAll(extraModes);
    }

    public State state(String route) {
        return routes.get(route);
    }

    /** Sleeps, hangs or throws as the route's mode says; a handler calls it per record. */
    public void apply(String route, @Nullable String key) throws InterruptedException {
        var state = routes.get(route);
        switch (state.mode) {
            case "fail" -> throw new RuntimeException("handler mode fail on route " + route);
            case "slow" -> Thread.sleep(state.delayMs);
            case "hang" -> hang(state);
            case "hang-hard" -> hangHard(state);
            default -> {}
        }
        if (key != null && state.failKeys.contains(key)) {
            throw new RuntimeException("failKeys has " + key + " on route " + route);
        }
    }

    /** For a route that is its own sink: {@link #apply} and the tag rule the target applies to the HTTP routes. */
    public void applyInProcess(String route, Event event) throws InterruptedException {
        apply(route, event.key());
        if (Payload.poisoned(event.tag()) && !routes.get(route).poisonFixed) {
            throw new OutboxPermanentException("tag " + event.tag() + " on route " + route + ": " + event.id());
        }
    }

    private static void hang(State state) throws InterruptedException {
        var started = System.nanoTime();
        while (state.mode.equals("hang") && System.nanoTime() - started < ONE_HOUR_NANOS) {
            Thread.sleep(50);
        }
    }

    private static void hangHard(State state) {
        var started = System.nanoTime();
        var interrupted = false;
        while (state.mode.equals("hang-hard") && System.nanoTime() - started < ONE_HOUR_NANOS) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Omitted parameters keep their value; {@code failKeys=} clears. */
    void update(@Nullable String route, @Nullable String mode, @Nullable String delayMs, @Nullable String failKeys,
            @Nullable String poisonFixed, @Nullable String waiting) {
        if (route != null && !routes.containsKey(route)) {
            throw new IllegalArgumentException("unknown route " + route + ", this app has " + routes.keySet());
        }
        if (mode != null && !modes.contains(mode)) {
            throw new IllegalArgumentException("unknown mode " + mode + ", this app has " + modes);
        }
        var delay = delayMs == null ? null : Long.valueOf(delayMs);
        for (var entry : routes.entrySet()) {
            if (route == null || route.equals(entry.getKey())) {
                var state = entry.getValue();
                if (delay != null) {
                    state.delayMs = delay;
                }
                if (failKeys != null) {
                    state.failKeys = Arrays.stream(failKeys.split(",")).filter(k -> !k.isBlank())
                            .collect(Collectors.toUnmodifiableSet());
                }
                if (poisonFixed != null) {
                    state.poisonFixed = Boolean.parseBoolean(poisonFixed);
                }
                if (waiting != null) {
                    state.waiting = Boolean.parseBoolean(waiting);
                }
                if (mode != null) {
                    state.mode = mode;
                }
            }
        }
    }

    Map<String, Object> asMap() {
        var map = new LinkedHashMap<String, Object>();
        routes.forEach((route, state) -> {
            var one = new LinkedHashMap<String, Object>();
            one.put("mode", state.mode);
            one.put("delayMs", state.delayMs);
            one.put("failKeys", state.failKeys);
            one.put("poisonFixed", state.poisonFixed);
            one.put("waiting", state.waiting);
            map.put(route, one);
        });
        return map;
    }
}
