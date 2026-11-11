package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DetachDropFixTest {

    @Test
    void aMinorBeforeTheFixOfDetachConcurrentlyAndDropNamesTheMinorThatHasIt() {
        assertThat(PartitionDdl.detachDropFix(14, 13)).isEqualTo("14.14");
        assertThat(PartitionDdl.detachDropFix(15, 8)).isEqualTo("15.9");
        assertThat(PartitionDdl.detachDropFix(16, 4)).isEqualTo("16.5");
        assertThat(PartitionDdl.detachDropFix(16, 0)).isEqualTo("16.5");
    }

    @Test
    void theMinorWithTheFixAndEveryLaterOneAndMajorPassSilently() {
        assertThat(PartitionDdl.detachDropFix(14, 14)).isNull();
        assertThat(PartitionDdl.detachDropFix(15, 9)).isNull();
        assertThat(PartitionDdl.detachDropFix(16, 5)).isNull();
        assertThat(PartitionDdl.detachDropFix(16, 11)).isNull();
        assertThat(PartitionDdl.detachDropFix(17, 0)).isNull();
        assertThat(PartitionDdl.detachDropFix(18, 0)).isNull();
    }
}
