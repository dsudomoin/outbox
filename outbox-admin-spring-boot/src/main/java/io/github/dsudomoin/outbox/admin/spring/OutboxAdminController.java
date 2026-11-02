package io.github.dsudomoin.outbox.admin.spring;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxStatus;
import io.github.dsudomoin.outbox.admin.spring.api.OutboxAdminApi;
import io.github.dsudomoin.outbox.admin.spring.model.AbandonWaiting;
import io.github.dsudomoin.outbox.admin.spring.model.Abandoned;
import io.github.dsudomoin.outbox.admin.spring.model.Count;
import io.github.dsudomoin.outbox.admin.spring.model.DeadLetter;
import io.github.dsudomoin.outbox.admin.spring.model.DeadLetterCount;
import io.github.dsudomoin.outbox.admin.spring.model.DeadLetterPage;
import io.github.dsudomoin.outbox.admin.spring.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.spring.model.Error;
import io.github.dsudomoin.outbox.admin.spring.model.Filter;
import io.github.dsudomoin.outbox.admin.spring.model.Header;
import io.github.dsudomoin.outbox.admin.spring.model.Instance;
import io.github.dsudomoin.outbox.admin.spring.model.Orphan;
import io.github.dsudomoin.outbox.admin.spring.model.OrphanKind;
import io.github.dsudomoin.outbox.admin.spring.model.Selection;
import io.github.dsudomoin.outbox.admin.spring.model.Status;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import java.beans.PropertyEditorSupport;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedRuntimeException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * The admin API over {@link OutboxAdmin}, each route a method of the contract's {@link OutboxAdminApi}. Failures become
 * JSON {@code Error} bodies by this class's own exception handlers, which Spring tries before any advice, so an app's
 * global advice, whatever its order, or Boot's problem details, do not answer for this controller, nor these handlers
 * for any other; they also answer what is thrown before a route's method runs (by an argument, or by a
 * {@code HandlerInterceptor} of its handler). A call's {@link IllegalArgumentException} answers 400, its
 * {@link IllegalStateException} 409, a request MVC could not read 400, any other exception MVC gives a status (a missing
 * parameter) that status, and anything else 500 with the failure's message, for the authenticated operator; a 5xx after
 * one WARN with the exception. Enums are mapped by exhaustive {@code switch}, so a constant added on either side fails
 * the build instead of a request. Every route is a method of this class, so a {@code HandlerInterceptor} that takes the
 * handlers of this bean type sees them all, before any body is read. Not final: an app's aspect over every controller
 * proxies this one too.
 */
@RestController
class OutboxAdminController implements OutboxAdminApi {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminController.class);

    /**
     * Request attribute: an operation's arguments for its audit line, a {@code Map<String, String>} by name in order,
     * values unescaped, set when the operation has some worth logging; there once the call completed.
     */
    static final String DETAIL = OutboxAdminController.class.getName() + ".detail";

    /** As the contract's {@code maxItems}: the library binds every id in each chunk's query. */
    private static final int MAX_IDS = 1000;
    /** As the Kora adapter writes them: always UTC, always three fraction digits. */
    private static final DateTimeFormatter DATE_TIME =
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private final OutboxAdmin admin;

    OutboxAdminController(OutboxAdmin admin) {
        this.admin = admin;
    }

    /** The query parameter by the contract's value, "pending"; MVC's own conversion takes the constant's name. */
    @InitBinder
    public void statusByItsContractValue(WebDataBinder binder) {
        binder.registerCustomEditor(DeadLetterStatus.class, new PropertyEditorSupport() {
            @Override
            public void setAsText(String text) {
                setValue(DeadLetterStatus.fromValue(text));
            }
        });
    }

    @Override
    public ResponseEntity<Status> getStatus() {
        return ResponseEntity.ok(status(admin.status()));
    }

    @Override
    public ResponseEntity<DeadLetterPage> findDeadLetters(@Nullable String topic, @Nullable String key,
            @Nullable String error, @Nullable String failedFrom, @Nullable String failedTo,
            @Nullable DeadLetterStatus status, @Nullable Long afterId, Integer limit) {
        var page = admin.findDeadLetters(filter(topic, key, error, failedFrom, failedTo, status), afterId, limit);
        return ResponseEntity.ok(new DeadLetterPage(page.letters().stream().map(OutboxAdminController::letter).toList())
            .nextAfterId(page.nextAfterId()));
    }

    @Override
    public ResponseEntity<Count> countDeadLetters(@Nullable String topic, @Nullable String key, @Nullable String error,
            @Nullable String failedFrom, @Nullable String failedTo, @Nullable DeadLetterStatus status) {
        return ResponseEntity.ok(new Count(admin.countDeadLetters(filter(topic, key, error, failedFrom, failedTo,
            status))));
    }

    @Override
    public ResponseEntity<Count> redriveDeadLetters(Selection selection) {
        var ids = ids(selection);
        return ResponseEntity.ok(new Count(ids != null ? admin.redriveDeadLetters(ids)
            : admin.redriveDeadLetters(filter(selection.getFilter()))));
    }

    @Override
    public ResponseEntity<Count> discardDeadLetters(Selection selection) {
        var ids = ids(selection);
        return ResponseEntity.ok(new Count(ids != null ? admin.discardDeadLetters(ids)
            : admin.discardDeadLetters(filter(selection.getFilter()))));
    }

    @Override
    public ResponseEntity<Count> restoreOrphan(String table, Long fromXid) {
        return ResponseEntity.ok(new Count(admin.restoreOrphan(table, fromXid)));
    }

    @Override
    public ResponseEntity<Count> recoverXidRegression() {
        return ResponseEntity.ok(new Count(admin.recoverXidRegression()));
    }

    @Override
    public ResponseEntity<Abandoned> abandonWaiting(AbandonWaiting body) {
        boolean dryRun = Boolean.TRUE.equals(body.getDryRun());
        var detail = new LinkedHashMap<String, String>(); // the reason goes into the audit line
        detail.put("topic", body.getTopic());
        detail.put("key", body.getKey());
        detail.put("reason", body.getReason());
        detail.put("dryRun", String.valueOf(dryRun));
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        var abandoned = admin.abandonWaiting(body.getTopic(),
            body.getKey() == null ? null : body.getKey().getBytes(UTF_8), body.getReason(), dryRun);
        return ResponseEntity.ok(new Abandoned(abandoned.keys(), abandoned.records()));
    }

    /**
     * What the contract's body routes' {@code consumes} turned down, here so it gets an {@code Error} and the guard
     * of every route: no {@code Content-Type} is a missing body, as for the Kora adapter; another one is no JSON.
     */
    @PostMapping(path = {"/kora-outbox/admin/v1/dead-letters/redrive", "/kora-outbox/admin/v1/dead-letters/discard",
            "/kora-outbox/admin/v1/waiting/abandon"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Error> notJson(@RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false)
            @Nullable String contentType) {
        return contentType == null
            ? ResponseEntity.badRequest().body(new Error("a JSON body is required, with Content-Type application/json"))
            : ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .body(new Error("Content-Type " + contentType + " is not supported: application/json only"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Error> refused(IllegalArgumentException e) {
        return error(HttpStatus.BAD_REQUEST.value(), message(e));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Error> conflict(IllegalStateException e) {
        return error(HttpStatus.CONFLICT.value(), message(e));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Error> badParameter(MethodArgumentTypeMismatchException e) {
        return error(HttpStatus.BAD_REQUEST.value(),
            "Query parameter '" + e.getName() + "' has invalid value: " + e.getValue());
    }

    /** The parser's own words; Spring's for a missing body would name this controller's method. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Error> unreadable(HttpMessageNotReadableException e) {
        return error(HttpStatus.BAD_REQUEST.value(),
            e.getCause() == null ? "a request body is required" : message(e.getMostSpecificCause()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Error> failed(Exception e, HttpServletRequest request) {
        int status = e instanceof ErrorResponse response ? response.getStatusCode().value() : 500;
        if (status >= 500) {
            log.warn("outbox admin {} \"{}\" failed, answered {}", request.getMethod(), request.getRequestURI(), status,
                e);
        }
        // a DataAccessException says "bad SQL grammar []"; the database's own words are the failure
        return error(status, message(e instanceof NestedRuntimeException nested ? nested.getMostSpecificCause() : e));
    }

    private static ResponseEntity<Error> error(int status, String message) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(new Error(message));
    }

    private static String message(Throwable e) {
        return Objects.requireNonNullElse(e.getMessage(), e.getClass().getName());
    }

    /** The ids, or null for the filter: exactly one of them. */
    @Nullable
    private static List<Long> ids(Selection selection) {
        if ((selection.getIds() == null) == (selection.getFilter() == null)) {
            throw new IllegalArgumentException("give exactly one of ids and filter");
        }
        var ids = selection.getIds();
        if (ids != null && ids.size() > MAX_IDS) {
            throw new IllegalArgumentException("at most " + MAX_IDS + " ids, not " + ids.size()
                + ": select more with a filter");
        }
        if (ids != null && ids.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("ids must be numbers, not null");
        }
        return ids;
    }

    /**
     * A bulk call's filter: a blank {@code topic} or {@code error} is refused, as dropped it would widen the call to
     * every letter the rest of the filter matches; here, so the 400 names the contract's field ({@link OutboxAdmin}
     * refuses it too).
     */
    private static DeadLetterFilter filter(Filter f) {
        return filter(notBlank("topic", f.getTopic()), f.getKey(), notBlank("error", f.getError()), f.getFailedFrom(),
            f.getFailedTo(), f.getStatus());
    }

    @Nullable
    private static String notBlank(String field, @Nullable String value) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException("a blank " + field + " in a filter matches every " + field
                + ": leave it out, or name one");
        }
        return value;
    }

    private static DeadLetterFilter filter(@Nullable String topic, @Nullable String key, @Nullable String error,
            @Nullable String failedFrom, @Nullable String failedTo, @Nullable DeadLetterStatus status) {
        return new DeadLetterFilter(topic, key == null ? null : key.getBytes(UTF_8), error,
            instant("failedFrom", failedFrom), instant("failedTo", failedTo),
            status == null ? null : letterStatus(status));
    }

    @Nullable
    private static Instant instant(String field, @Nullable String value) {
        try {
            return value == null ? null : OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(field + " is no date-time with an offset: " + value);
        }
    }

    private static io.github.dsudomoin.outbox.DeadLetter.Status letterStatus(DeadLetterStatus status) {
        return switch (status) {
            case PENDING -> io.github.dsudomoin.outbox.DeadLetter.Status.PENDING;
            case KEPT -> io.github.dsudomoin.outbox.DeadLetter.Status.KEPT;
            case SCHEDULED -> io.github.dsudomoin.outbox.DeadLetter.Status.SCHEDULED;
        };
    }

    private static Status status(OutboxStatus s) {
        return new Status().halted(s.halted()).lagSeconds(s.lagSeconds()).retrySize(s.retrySize())
            .parkedSize(s.parkedSize()).waitingSize(s.waitingSize()).waitingOldestSeconds(s.waitingOldestSeconds())
            .deadLetters(s.deadLetters().stream()
                .map(c -> new DeadLetterCount(c.topic(), status(c.status()), c.count())).toList())
            .orphans(s.orphans().stream().map(o -> new Orphan(o.table(), kind(o.kind()))).toList())
            .tailOverflow(s.tailOverflow()).premade(s.premade()).xidsLeft(s.xidsLeft())
            .maintenanceSinceSuccessSeconds(s.maintenanceSinceSuccessSeconds()).shards(s.shards())
            .instances(s.instances().stream().map(OutboxAdminController::instance).toList());
    }

    private static Instance instance(OutboxStatus.Instance i) {
        return new Instance(i.id(), i.shards(), dateTime(i.lastSeen()), i.hookActive(), i.relay())
            .contracts(i.contracts() == null ? null : new TreeMap<>(i.contracts())).routes(i.routes());
    }

    private static DeadLetter letter(io.github.dsudomoin.outbox.DeadLetter l) {
        return new DeadLetter().id(l.id()).messageId(l.messageId()).topic(l.topic()).key(l.key()).payload(l.payload())
            .headers(l.headers().stream().map(h -> new Header(h.name()).value(h.value())).toList())
            .createdAt(dateTime(l.createdAt())).attempts(l.attempts()).failedAt(dateTime(l.failedAt())).error(l.error())
            .status(status(l.status())).redrives(l.redrives()).handlerAttempts(l.handlerAttempts())
            .dueAt(l.dueAt() == null ? null : dateTime(l.dueAt()));
    }

    private static DeadLetterStatus status(io.github.dsudomoin.outbox.DeadLetter.Status status) {
        return switch (status) {
            case PENDING -> DeadLetterStatus.PENDING;
            case KEPT -> DeadLetterStatus.KEPT;
            case SCHEDULED -> DeadLetterStatus.SCHEDULED;
        };
    }

    private static OrphanKind kind(OutboxStatus.OrphanKind kind) {
        return switch (kind) {
            case DETACHED_BY_LIBRARY -> OrphanKind.DETACHED_BY_LIBRARY;
            case FOREIGN -> OrphanKind.FOREIGN;
            case SET_ASIDE -> OrphanKind.SET_ASIDE;
        };
    }

    private static String dateTime(Instant instant) {
        return DATE_TIME.format(instant);
    }
}
