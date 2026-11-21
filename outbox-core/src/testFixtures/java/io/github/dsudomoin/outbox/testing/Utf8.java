package io.github.dsudomoin.outbox.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Strings as the bytes the tests write. */
public final class Utf8 {

    private Utf8() {}

    /** {@code s} in UTF-8. */
    public static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
