package io.github.dsudomoin.outbox.testing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestOutboxConfigTest {

    @Test
    void testConfigDefaultsToIsolatedTables() {
        var config = new TestOutboxConfig();

        assertThat(config.table()).matches("ob_[0-9a-f]{10}").isNotEqualTo(new TestOutboxConfig().table());
        assertThat(config.inbox().table()).matches("ob_[0-9a-f]{10}").isNotEqualTo(config.table());
    }
}
