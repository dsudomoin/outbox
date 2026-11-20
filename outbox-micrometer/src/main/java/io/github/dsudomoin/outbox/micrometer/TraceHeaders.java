package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxHeader;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Checks of stored trace headers, made on the bytes in place, as the delivery's getter reads them: the last header of
 * a name wins. Only W3C trace context and B3 (single and multi header) are recognized.
 */
final class TraceHeaders {

    static final String TRACEPARENT = "traceparent";
    static final String B3 = "b3";
    static final String B3_TRACE_ID = "X-B3-TraceId";
    static final String B3_SPAN_ID = "X-B3-SpanId";

    private TraceHeaders() {}

    /** The value of the last header named {@code name}, or null when there is none or its value is null. */
    @Nullable
    static byte[] last(List<OutboxHeader> headers, String name) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            var h = headers.get(i);
            if (h.name().equals(name)) {
                return h.value();
            }
        }
        return null;
    }

    /**
     * Whether a header of this name, stored by the message itself, gives it its own parent: any trace field but
     * {@code baggage} and {@code tracestate}, which carry no parent of their own.
     */
    static boolean carriesParent(String name) {
        return !name.equals("baggage") && !name.equals("tracestate");
    }

    /**
     * A well-formed W3C trace context: {@code version-traceid-parentid-flags} in lowercase hex, version not {@code ff},
     * ids not all zero, nothing after the flags in version {@code 00}, a later version's extra fields after a
     * {@code -}.
     */
    static boolean wellFormedTraceparent(@Nullable byte[] v) {
        if (v == null || v.length < 55 || v[2] != '-' || v[35] != '-' || v[52] != '-'
                || v.length > 55 && v[55] != '-') {
            return false;
        }
        int version = hexByte(v, 0);
        return version >= 0 && version != 0xff && (version != 0 || v.length == 55)
                && hexNotZero(v, 3, 35) && hexNotZero(v, 36, 52) && hexByte(v, 53) >= 0;
    }

    /** The trace flags of a {@link #wellFormedTraceparent} value. */
    static int traceFlags(byte[] traceparent) {
        return hexByte(traceparent, 53);
    }

    /**
     * A B3 single header with a parent: {@code traceid-spanid}, then at most a sampling state and a parent span id,
     * each after a {@code -}; the trace id 16 or 32 lowercase hex digits, the span id 16, neither all zero. A bare
     * sampling state ({@code 0}, {@code 1}, {@code d}) carries none.
     */
    static boolean wellFormedB3(@Nullable byte[] v) {
        if (v == null) {
            return false;
        }
        int dash = indexOf(v, 0);
        if (dash != 16 && dash != 32 || !hexNotZero(v, 0, dash) || v.length < dash + 17
                || !hexNotZero(v, dash + 1, dash + 17) || v.length > dash + 17 && v[dash + 17] != '-') {
            return false;
        }
        int dashes = 1;
        for (int i = dash + 17; i < v.length; i++) {
            if (v[i] == '-' && ++dashes > 3) {
                return false;
            }
        }
        return true;
    }

    /** B3 multi headers with a parent: a trace id of 16 or 32 lowercase hex digits and a span id of 16, not all zero. */
    static boolean wellFormedB3Ids(@Nullable byte[] traceId, @Nullable byte[] spanId) {
        return traceId != null && spanId != null && (traceId.length == 16 || traceId.length == 32)
                && hexNotZero(traceId, 0, traceId.length) && spanId.length == 16 && hexNotZero(spanId, 0, 16);
    }

    private static int indexOf(byte[] v, int from) {
        for (int i = from; i < v.length; i++) {
            if (v[i] == '-') {
                return i;
            }
        }
        return -1;
    }

    /** Two lowercase hex digits at {@code at}, or -1. */
    private static int hexByte(byte[] v, int at) {
        int hi = hexDigit(v[at]);
        int lo = hexDigit(v[at + 1]);
        return hi < 0 || lo < 0 ? -1 : hi << 4 | lo;
    }

    /** Lowercase hex digits in {@code [from, to)}, not all zero. */
    private static boolean hexNotZero(byte[] v, int from, int to) {
        boolean notZero = false;
        for (int i = from; i < to; i++) {
            int d = hexDigit(v[i]);
            if (d < 0) {
                return false;
            }
            notZero |= d != 0;
        }
        return notZero;
    }

    private static int hexDigit(byte b) {
        return b >= '0' && b <= '9' ? b - '0' : b >= 'a' && b <= 'f' ? b - 'a' + 10 : -1;
    }
}
