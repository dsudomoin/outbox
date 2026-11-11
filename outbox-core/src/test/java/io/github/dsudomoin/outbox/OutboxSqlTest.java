package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import org.junit.jupiter.api.Test;

class OutboxSqlTest {

    private final TestOutboxConfig config = new TestOutboxConfig().table("app.orders_outbox").inboxTable("app.inbox");

    @Test
    void roleMustBeAnIdentifier() {
        for (var role : new String[] {"app; drop table t", "", "1app"}) {
            assertThatThrownBy(() -> OutboxSql.outbox(config, role)).as(role)
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> OutboxSql.inbox(config, role)).as(role)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(OutboxSql.outbox(config, "app_role")).contains("to app_role;");
    }

    @Test
    void noGrantsWithoutRole() {
        assertThat(OutboxSql.outbox(config, null)).doesNotContainIgnoringCase("grant");
        assertThat(OutboxSql.inbox(config, null)).doesNotContainIgnoringCase("grant");
    }

    @Test
    void grantsNameTheRole() {
        assertThat(OutboxSql.outbox(config, "app"))
                .contains("grant select, insert, update, delete on table app.orders_outbox, app.orders_outbox_tick,")
                .contains("grant select on sequence app.orders_outbox_id_seq to app;");
        assertThat(OutboxSql.inbox(config, "app"))
                .contains("grant select, insert, update, delete on table app.inbox to app;");
    }

    @Test
    void aKeywordTheUnquotedSqlCannotTakeIsRefusedBeforeRendering() {
        for (var table : new String[] {"order.outbox", "user", "Left"}) {
            assertThatThrownBy(() -> OutboxSql.outbox(new TestOutboxConfig().table(table), null)).as(table)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("is a reserved Postgres keyword");
            assertThatThrownBy(() -> OutboxSql.inbox(new TestOutboxConfig().inboxTable(table), null)).as(table)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("is a reserved Postgres keyword");
        }
        // a keyword after the schema, and one the grammar takes as a table name, stay accepted
        assertThat(OutboxSql.outbox(new TestOutboxConfig().table("public.user"), null))
                .contains("create table public.user (");
        assertThat(OutboxSql.inbox(new TestOutboxConfig().inboxTable("between"), null)).contains("between");
        // a keyword only since Postgres 16 is left to the server's own list, as 14 and 15 take it as a name
        assertThat(OutboxSql.outbox(new TestOutboxConfig().table("system_user"), null))
                .contains("create table system_user (");
    }
}
