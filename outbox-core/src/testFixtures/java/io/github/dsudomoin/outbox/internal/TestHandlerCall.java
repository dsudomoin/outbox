package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.Outcome;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Runs code as the relay runs a handler call, for tests of handlers outside this package: {@link #windDown} is what the
 * relay does at the soft deadline and again just before it cancels the call, and {@link ShardProcessor#callDeadline()}
 * is {@code batchTimeout} from the construction. Nothing cancels the call: the test does what the relay would.
 */
public final class TestHandlerCall {

    private final ShardProcessor.HandlerCall call;

    public TestHandlerCall(Duration batchTimeout, Callable<List<Outcome>> body) {
        this.call = new ShardProcessor.HandlerCall(body, System.nanoTime() + batchTimeout.toNanos());
    }

    /** Runs the call on this thread. */
    public List<Outcome> call() throws Exception {
        return call.call();
    }

    /** Asks the call to wind down and interrupts it, as the relay does. */
    public void windDown() {
        call.interrupt();
    }
}
