package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxRecord;
import java.util.Collection;

/**
 * What {@code outbox.relay.batchBytes} counts of a row: its payload, key and headers as stored, a null as 0. The
 * statements measure it with {@code octet_length}, which reads a TOASTed value's size from its pointer without
 * fetching it, and cut where a running sum of it passes the budget, so the rows past it are neither fetched nor sent.
 */
final class RowBytes {

    /** No byte limit: more than any call holds, and four times it is still a long. */
    static final long NO_LIMIT = Long.MAX_VALUE / 4;

    private RowBytes() {}

    /** The budget of {@code outbox.relay.batchBytes}: 0 is no limit. */
    static long budget(long batchBytes) {
        return batchBytes == 0 ? NO_LIMIT : Math.min(batchBytes, NO_LIMIT);
    }

    /**
     * The SQL of a row's bytes, of the table or subquery {@code alias}: int4, as a row read whole is below the
     * protocol's 2 GB message anyway.
     */
    static String sql(String alias) {
        return "(coalesce(octet_length(" + alias + ".payload), 0) + coalesce(octet_length(" + alias + ".key), 0)"
                + " + coalesce(octet_length(" + alias + ".headers), 0))";
    }

    /**
     * The rows of {@code ordered} that fit in a byte budget, in its order: a query of {@code columns} and {@code b},
     * the row's {@link #sql bytes}, ordered by {@code order} (over its columns as {@code q.<column>}) and limited.
     * Rows go while their running sum stays within the budget, the first always when one at least is asked for.
     * Parameters: those of {@code ordered}, then the budget and whether one at least.
     */
    static String upTo(String ordered, String order, String columns) {
        return "select " + columns + " from (select q.*, sum(q.b) over w as run, row_number() over w as n from ("
                + ordered + ") q window w as (order by " + order + " rows between unbounded preceding and current"
                + " row)) q where q.run <= ? or q.n = 1 and ? order by " + order;
    }

    static long of(OutboxRecord r) {
        return (r.payload() == null ? 0 : r.payload().length) + (r.key() == null ? 0 : r.key().length)
                + HeaderCodec.size(r.headers());
    }

    static long of(Collection<OutboxRecord> rows) {
        long bytes = 0;
        for (var r : rows) {
            bytes += of(r);
        }
        return bytes;
    }
}
