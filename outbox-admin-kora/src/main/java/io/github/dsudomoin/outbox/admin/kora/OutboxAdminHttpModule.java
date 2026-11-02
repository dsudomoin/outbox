package io.github.dsudomoin.outbox.admin.kora;

import io.github.dsudomoin.outbox.InternalApi;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiController;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiControllerModule;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.AbandonWaitingApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.CountDeadLettersApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.DiscardDeadLettersApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.FindDeadLettersApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.GetStatusApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.RecoverStrandedShardsApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.RecoverXidRegressionApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.RedriveDeadLettersApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiServerResponseMappers.RestoreOrphanApiResponseMapper;
import io.github.dsudomoin.outbox.admin.kora.model.Abandoned;
import io.github.dsudomoin.outbox.admin.kora.model.AbandonWaiting;
import io.github.dsudomoin.outbox.admin.kora.model.Count;
import io.github.dsudomoin.outbox.admin.kora.model.DeadLetterPage;
import io.github.dsudomoin.outbox.admin.kora.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.kora.model.Filter;
import io.github.dsudomoin.outbox.admin.kora.model.ModelError;
import io.github.dsudomoin.outbox.admin.kora.model.OrphanKind;
import io.github.dsudomoin.outbox.admin.kora.model.Selection;
import io.github.dsudomoin.outbox.admin.kora.model.Status;
import java.time.OffsetDateTime;
import java.util.List;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;
import ru.tinkoff.kora.http.server.common.handler.HttpServerResponseMapper;
import ru.tinkoff.kora.http.server.common.handler.StringParameterReader;
import ru.tinkoff.kora.json.common.annotation.Json;
import ru.tinkoff.kora.json.common.JsonReader;
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * The admin HTTP API on the app's public HTTP server, routes under {@code /kora-outbox/admin/v1}; contract at
 * {@code openapi/kora-outbox-admin.yaml} on the classpath. Needs {@code OutboxModule}, an HTTP server module, a JSON
 * module and one {@link OutboxAdminAuthorizer} component.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, UndertowHttpServerModule, JsonModule,
 *         OutboxAdminHttpModule {}
 * }</pre>
 */
public interface OutboxAdminHttpModule extends OutboxAdminApiControllerModule {

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default OutboxAdminInterceptor outboxAdminInterceptor(OutboxAdminAuthorizer authorizer,
            BlockingRequestExecutor executor, JsonWriter<ModelError> errorWriter) {
        return new OutboxAdminInterceptor(authorizer, executor, errorWriter);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default OutboxAdminApiDelegate outboxAdminDelegate(OutboxAdmin admin) {
        return new OutboxAdminDelegate(admin);
    }

    // The generator's enum readers and writers (by the contract's values, "pending", not the constant's name) are
    // @Component classes, which an app's graph never finds in a library jar: without these it falls back to
    // Enum::name for the query parameter and generates its own JSON codecs, with a warning, in every app build.

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default StringParameterReader<DeadLetterStatus> outboxAdminDeadLetterStatusParameterReader() {
        return new DeadLetterStatus.StringParameterReader();
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonReader<DeadLetterStatus> outboxAdminDeadLetterStatusJsonReader(JsonReader<String> strings) {
        return new DeadLetterStatus.JsonReader(strings);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonWriter<DeadLetterStatus> outboxAdminDeadLetterStatusJsonWriter(JsonWriter<String> strings) {
        return new DeadLetterStatus.JsonWriter(strings);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonWriter<OrphanKind> outboxAdminOrphanKindJsonWriter(JsonWriter<String> strings) {
        return new OrphanKind.JsonWriter(strings);
    }

    /**
     * Internal type, unstable: for the adapters of this repository. Refuses a field the contract does not name, which
     * the generated reader would skip.
     */
    @InternalApi
    default JsonReader<Selection> outboxAdminSelectionJsonReader(JsonReader<List<Long>> ids,
            JsonReader<Filter> filter) {
        return RequestReaders.selection(ids, filter);
    }

    /**
     * Internal type, unstable: for the adapters of this repository. Refuses a field the contract does not name, which
     * the generated reader would skip.
     */
    @InternalApi
    default JsonReader<Filter> outboxAdminFilterJsonReader(JsonReader<OffsetDateTime> times,
            JsonReader<DeadLetterStatus> statuses) {
        return RequestReaders.filter(times, statuses);
    }

    /**
     * Internal type, unstable: for the adapters of this repository. Refuses a field the contract does not name, which
     * the generated reader would skip: a typo of {@code key} would abandon every key of the topic.
     */
    @InternalApi
    default JsonReader<AbandonWaiting> outboxAdminAbandonWaitingJsonReader() {
        return RequestReaders.abandonWaiting();
    }

    // The generated controller and response mappers: the Java processor creates them from their constructors, KSP
    // creates no Java class of a library jar that way, and a Kotlin app would not build without these.

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default OutboxAdminApiController outboxAdminApiController(OutboxAdminApiDelegate delegate) {
        return new OutboxAdminApiController(delegate);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default AbandonWaitingApiResponseMapper outboxAdminAbandonWaitingApiResponseMapper(
            @Json HttpServerResponseMapper<Abandoned> abandoned, @Json HttpServerResponseMapper<ModelError> error) {
        return new AbandonWaitingApiResponseMapper(abandoned, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default CountDeadLettersApiResponseMapper outboxAdminCountDeadLettersApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new CountDeadLettersApiResponseMapper(count, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default DiscardDeadLettersApiResponseMapper outboxAdminDiscardDeadLettersApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new DiscardDeadLettersApiResponseMapper(count, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default FindDeadLettersApiResponseMapper outboxAdminFindDeadLettersApiResponseMapper(
            @Json HttpServerResponseMapper<DeadLetterPage> page, @Json HttpServerResponseMapper<ModelError> error) {
        return new FindDeadLettersApiResponseMapper(page, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default GetStatusApiResponseMapper outboxAdminGetStatusApiResponseMapper(
            @Json HttpServerResponseMapper<Status> status, @Json HttpServerResponseMapper<ModelError> error) {
        return new GetStatusApiResponseMapper(status, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default RecoverStrandedShardsApiResponseMapper outboxAdminRecoverStrandedShardsApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new RecoverStrandedShardsApiResponseMapper(count, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default RecoverXidRegressionApiResponseMapper outboxAdminRecoverXidRegressionApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new RecoverXidRegressionApiResponseMapper(count, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default RedriveDeadLettersApiResponseMapper outboxAdminRedriveDeadLettersApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new RedriveDeadLettersApiResponseMapper(count, error, error, error, error, error);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default RestoreOrphanApiResponseMapper outboxAdminRestoreOrphanApiResponseMapper(
            @Json HttpServerResponseMapper<Count> count, @Json HttpServerResponseMapper<ModelError> error) {
        return new RestoreOrphanApiResponseMapper(count, error, error, error, error, error);
    }
}
