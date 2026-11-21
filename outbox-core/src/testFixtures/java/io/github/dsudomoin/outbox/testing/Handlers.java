package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import java.util.List;
import java.util.function.Function;

/** Handlers the tests build in one line. */
public final class Handlers {

    private Handlers() {}

    /** A handler whose batch call answers with {@code answer} of the whole batch. */
    public static OutboxHandler answering(Function<List<OutboxRecord>, List<Outcome>> answer) {
        return new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return answer.apply(records);
            }
        };
    }

    /** A handler whose batch call answers each record with {@code outcome} of it, in order. */
    public static OutboxHandler eachRecord(Function<OutboxRecord, Outcome> outcome) {
        return answering(records -> records.stream().map(outcome).toList());
    }
}
