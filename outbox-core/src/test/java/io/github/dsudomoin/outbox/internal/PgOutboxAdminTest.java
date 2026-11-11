package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PgOutboxAdminTest {

    /** A bidi override or a zero-width character would reorder or hide part of the log line on screen. */
    @Test
    void printableEscapesFormatCharacters() {
        assertThat(PgOutboxAdmin.printable("x‮y​z⁦󠀁\n"))
                .isEqualTo("x\\u202ey\\u200bz\\u2066\\udb40\\udc01\\n");
    }
}
