package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.LogLimit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;

/**
 * The calls a handler that hands its records on ({@link OutboxHandler#byTopic}, {@link LocalOnly}) makes to the
 * handlers behind it: a call that fails fails only the records it was handed. One per such handler, for its log limit.
 */
final class RouteCalls {

    private static final long REPORT_INTERVAL = TimeUnit.SECONDS.toNanos(10);

    /** What the ERROR calls the handlers behind it. */
    private final String what;
    /** At most one invalid answer logged per interval, the rest counted. */
    private final LogLimit reports = new LogLimit(REPORT_INTERVAL);

    RouteCalls(String what) {
        this.what = what;
    }

    /**
     * {@code route}'s outcomes for {@code records}, one per record. A route that throws {@link OutboxWaitException}
     * gives {@code Waiting} (its kind and message) for all of them; one that throws another exception, or returns an
     * invalid list (null, wrong size, a null element; logged at ERROR, at most once per 10 s), gives {@code Retry} for
     * all of them. An {@link Error} propagates, and when {@code records} is the {@code whole} call so does any
     * exception but a wait, and an invalid list is thrown as an {@link IllegalStateException}: the relay then treats
     * it as the failed call it is (a lone record's {@link OutboxPermanentException} goes to dead letters, a call of
     * several is halved until the failing one is alone), which per-record {@code Retry} would hide.
     */
    List<Outcome> handleBatch(OutboxHandler route, List<OutboxRecord> records, boolean whole) {
        List<Outcome> outcomes;
        try {
            outcomes = route.handleBatch(records);
        } catch (OutboxWaitException e) { // the route cannot take any of them now
            Outcome waiting = new Outcome.Waiting(e.kind(), e.getMessage(), null);
            outcomes = records.stream().map(r -> waiting).toList();
        } catch (Exception e) { // one broken route must not fail the records of others; Errors propagate
            if (whole) {
                throw e;
            }
            outcomes = records.stream().map(r -> Outcome.retry(e)).toList();
        }
        if (outcomes == null || outcomes.size() != records.size()
                || outcomes.stream().anyMatch(Objects::isNull)) { // a handler bug: Retry, not the call
            var error = new IllegalStateException(route + " returned an invalid outcome list for " + records.size()
                    + " records: " + outcomes);
            report(error);
            if (whole) {
                throw error;
            }
            outcomes = records.stream().map(r -> Outcome.retry(error)).toList();
        }
        return outcomes;
    }

    private void report(IllegalStateException error) {
        int suppressed = reports.pass();
        if (suppressed < 0) {
            return;
        }
        LoggerFactory.getLogger(OutboxHandler.class).error("outbox " + what + " failed; its records are retried ({}"
                + " more since the previous report not logged)", suppressed, error);
    }
}
