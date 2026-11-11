package io.github.dsudomoin.outbox.admin.spring;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxStatus;
import io.github.dsudomoin.outbox.admin.spring.api.OutboxAdminApi;
import io.github.dsudomoin.outbox.admin.spring.model.AbandonKey;
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
import java.util.stream.Collectors;
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
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * The admin API over {@link OutboxAdmin}, each route a method of the contract's {@link OutboxAdminApi}. Failures become
 * JSON {@code Error} bodies by this class's own exception handlers, which Spring tries before any advice, so an app's
 * global advice, whatever its order, or Boot's problem details, do not answer for this controller, nor these handlers
 * for any other; they also answer what is thrown before a route's method runs (by an argument, or by a
 * {@code HandlerInterceptor} of its handler), then a fixed 500 for a caller the guard has not let through, whatever the
 * failure. A call's {@link IllegalArgumentException} answers 400, its {@link IllegalStateException} 409, a request MVC
 * could not read 400, any other exception MVC gives a status (a missing parameter) that status, and anything else 500
 * with the failure's message, for the authenticated operator; a 5xx after one WARN with the exception. Enums are mapped
 * by exhaustive {@code switch}, so a constant added on either side fails the build instead of a request. Every route is
 * a method of this class, so {@link OutboxAdminGuard}, which takes the handlers of this bean type, sees them all,
 * before it reads a JSON body, and each refuses a call the guard did not allow, should a mapping have lost it. Not final:
 * an app's aspect over every controller proxies this one too.
 */
@RestController
class OutboxAdminController implements OutboxAdminApi {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminController.class);

    /**
     * Request attribute: an operation's arguments for its audit line, a {@code Map<String, String>} by name in order,
     * values unescaped, set when the operation has some worth logging; {@link OutboxAdminGuard} reads it once the call
     * completed.
     */
    static final String DETAIL = OutboxAdminController.class.getName() + ".detail";

    /** As the contract's {@code maxItems}: the library binds every id in each chunk's query. */
    private static final int MAX_IDS = 1000;
    /** The ids of a call its audit line shows, after their count: the line stays short for a thousand of them. */
    private static final int AUDIT_IDS = 10;
    /** As the Kora adapter writes them: always UTC, always three fraction digits. */
    private static final DateTimeFormatter DATE_TIME =
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private final OutboxAdmin admin;

    OutboxAdminController(OutboxAdmin admin) {
        this.admin = admin;
    }

    /**
     * Runs ahead of each route's arguments, so before this controller reads a JSON body: losing the guard must not open
     * a route, by any path. Holds with the guard on the mapping too, where the guard has already refused anyone it did
     * not allow. An app can skip this (its own {@code HandlerAdapter}, an aspect swallowing the refusal, a model entry
     * under this method's empty name), so {@link #admin} checks again.
     */
    @ModelAttribute
    public void guardRan(HttpServletRequest request) {
        OutboxAdminGuard.requireAllowed(request);
    }

    /** The one way to the admin, for every route: nothing an app does around a route's method gets past this. */
    private OutboxAdmin admin() {
        OutboxAdminGuard.requireAllowed(
            ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest());
        return admin;
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
        return ResponseEntity.ok(status(admin().status()));
    }

    @Override
    public ResponseEntity<DeadLetterPage> findDeadLetters(@Nullable String topic, @Nullable String key,
            @Nullable String error, @Nullable String failedFrom, @Nullable String failedTo,
            @Nullable DeadLetterStatus status, @Nullable Long afterId, Integer limit) {
        var page = admin().findDeadLetters(filter(topic, key, error, failedFrom, failedTo, status), afterId, limit);
        return ResponseEntity.ok(new DeadLetterPage(page.letters().stream().map(OutboxAdminController::letter).toList())
            .nextAfterId(page.nextAfterId()));
    }

    @Override
    public ResponseEntity<Count> countDeadLetters(@Nullable String topic, @Nullable String key, @Nullable String error,
            @Nullable String failedFrom, @Nullable String failedTo, @Nullable DeadLetterStatus status) {
        return ResponseEntity.ok(new Count(admin().countDeadLetters(filter(topic, key, error, failedFrom, failedTo,
            status))));
    }

    @Override
    public ResponseEntity<Count> redriveDeadLetters(Selection parameter) {
        var selection = RequestReaders.read(Selection.class);
        var ids = ids(selection);
        return ResponseEntity.ok(new Count(ids != null ? admin().redriveDeadLetters(audited(ids))
            : admin().redriveDeadLetters(audited(selection.getFilter()))));
    }

    @Override
    public ResponseEntity<Count> discardDeadLetters(Selection parameter) {
        var selection = RequestReaders.read(Selection.class);
        var ids = ids(selection);
        return ResponseEntity.ok(new Count(ids != null ? admin().discardDeadLetters(audited(ids))
            : admin().discardDeadLetters(audited(selection.getFilter()))));
    }

    @Override
    public ResponseEntity<Count> restoreOrphan(String table, Long fromXid) {
        var detail = new LinkedHashMap<String, String>(); // the table is in the path already
        detail.put("fromXid", String.valueOf(fromXid));
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        return ResponseEntity.ok(new Count(admin().restoreOrphan(table, fromXid)));
    }

    @Override
    public ResponseEntity<Count> recoverXidRegression() {
        return ResponseEntity.ok(new Count(admin().recoverXidRegression()));
    }

    @Override
    public ResponseEntity<Count> recoverStrandedShards() {
        return ResponseEntity.ok(new Count(admin().recoverStrandedShards()));
    }

    @Override
    public ResponseEntity<Abandoned> abandonKey(AbandonKey parameter) {
        var body = RequestReaders.read(AbandonKey.class);
        boolean dryRun = Boolean.TRUE.equals(body.getDryRun());
        var detail = new LinkedHashMap<String, String>(); // the reason goes into the audit line
        detail.put("topic", body.getTopic());
        detail.put("key", body.getKey());
        detail.put("messageId", body.getMessageId() == null ? null : body.getMessageId().toString());
        detail.put("reason", body.getReason());
        detail.put("dryRun", String.valueOf(dryRun));
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        var abandoned = body.getMessageId() != null
            ? admin().abandonRecord(body.getTopic(), body.getMessageId(), body.getReason(), dryRun)
            : admin().abandonKey(body.getTopic(), body.getKey().getBytes(UTF_8), body.getReason(), dryRun);
        return ResponseEntity.ok(new Abandoned(abandoned.keys(), abandoned.records()));
    }

    @Override
    public ResponseEntity<Abandoned> abandonWaiting(AbandonWaiting parameter) {
        var body = RequestReaders.read(AbandonWaiting.class);
        boolean dryRun = Boolean.TRUE.equals(body.getDryRun());
        var detail = new LinkedHashMap<String, String>(); // the reason goes into the audit line
        detail.put("topic", body.getTopic());
        detail.put("key", body.getKey());
        detail.put("reason", body.getReason());
        detail.put("dryRun", String.valueOf(dryRun));
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        var abandoned = admin().abandonWaiting(body.getTopic(),
            body.getKey() == null ? null : body.getKey().getBytes(UTF_8), body.getReason(), dryRun);
        return ResponseEntity.ok(new Abandoned(abandoned.keys(), abandoned.records()));
    }

    /**
     * What the contract's body routes' {@code consumes} turned down, here so it gets an {@code Error} and the guard
     * of every route: no {@code Content-Type} is a missing body, as for the Kora adapter; another one is no JSON.
     */
    @PostMapping(path = {"/kora-outbox/admin/v1/dead-letters/redrive", "/kora-outbox/admin/v1/dead-letters/discard",
            "/kora-outbox/admin/v1/waiting/abandon", "/kora-outbox/admin/v1/keys/abandon"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Error> notJson(@RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false)
            @Nullable String contentType) {
        return contentType == null
            ? ResponseEntity.badRequest().body(new Error("a JSON body is required, with Content-Type application/json"))
            : ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .body(new Error("Content-Type " + contentType + " is not supported: application/json only"));
    }

    /** The guard's answer, as it decided, already in the audit log. */
    @ExceptionHandler(OutboxAdminGuard.Refused.class)
    public ResponseEntity<Error> denied(OutboxAdminGuard.Refused e) {
        return error(e.status, e.getMessage());
    }

    /** One way out for every failure, so that none shows its text to a caller the guard has not let through. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Error> failed(Exception e, HttpServletRequest request) {
        if (!OutboxAdminGuard.allowed(request)) { // an app's interceptor ahead of the guard threw
            log.warn("outbox admin {} failed, answered 500", OutboxAdminGuard.caller(request), e);
            OutboxAdminGuard.auditDenied(request);
            return error(500, "authorization failed");
        }
        if (e instanceof IllegalArgumentException) {
            return error(HttpStatus.BAD_REQUEST.value(), message(e));
        }
        if (e instanceof IllegalStateException) {
            return error(HttpStatus.CONFLICT.value(), message(e));
        }
        if (e instanceof MethodArgumentTypeMismatchException mismatch) {
            return error(HttpStatus.BAD_REQUEST.value(),
                "Query parameter '" + mismatch.getName() + "' has invalid value: " + mismatch.getValue());
        }
        if (e instanceof HttpMessageNotReadableException unreadable) { // the parser's words; Spring's name this method
            return error(HttpStatus.BAD_REQUEST.value(),
                e.getCause() == null ? "a request body is required" : message(unreadable.getMostSpecificCause()));
        }
        int status = e instanceof ErrorResponse response ? response.getStatusCode().value() : 500;
        if (status >= 500) {
            log.warn("outbox admin {} failed, answered {}", OutboxAdminGuard.caller(request), status, e);
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

    /** {@code ids}, their count and the first {@value #AUDIT_IDS} of them put into the audit line. */
    private static List<Long> audited(List<Long> ids) {
        var detail = new LinkedHashMap<String, String>();
        detail.put("count", String.valueOf(ids.size()));
        detail.put("ids", ids.stream().limit(AUDIT_IDS).map(String::valueOf)
            .collect(Collectors.joining(", ", "[", ids.size() > AUDIT_IDS ? ", ...]" : "]")));
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        return ids;
    }

    /** {@link #filter(Filter)}, its fields put into the audit line. */
    private static DeadLetterFilter audited(Filter f) {
        var filter = filter(f);
        var detail = new LinkedHashMap<String, String>();
        detail.put("topic", filter.topic());
        detail.put("key", f.getKey());
        detail.put("error", filter.errorContains());
        detail.put("failedFrom", filter.failedFrom() == null ? null : filter.failedFrom().toString());
        detail.put("failedTo", filter.failedTo() == null ? null : filter.failedTo().toString());
        detail.put("status", f.getStatus() == null ? null : f.getStatus().toString());
        RequestContextHolder.currentRequestAttributes().setAttribute(DETAIL, detail, RequestAttributes.SCOPE_REQUEST);
        return filter;
    }

    /** A bulk call's filter, as {@link RequestReaders} read it from the body: no field of it is a mapper's. */
    private static DeadLetterFilter filter(Filter f) {
        return filter(f.getTopic(), f.getKey(), f.getError(), f.getFailedFrom(), f.getFailedTo(), f.getStatus());
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
            .deadLetters(s.deadLetters() == null ? null : s.deadLetters().stream()
                .map(c -> new DeadLetterCount(c.topic(), status(c.status()), c.count())).toList())
            .orphans(s.orphans().stream().map(o -> new Orphan(o.table(), kind(o.kind()))).toList())
            .tailOverflow(s.tailOverflow()).premade(s.premade()).xidsLeft(s.xidsLeft())
            .maintenanceSinceSuccessSeconds(s.maintenanceSinceSuccessSeconds()).shards(s.shards())
            .instances(s.instances().stream().map(OutboxAdminController::instance).toList());
    }

    private static Instance instance(OutboxStatus.Instance i) {
        return new Instance(i.id(), i.shards(), dateTime(i.lastSeen()), i.hookActive(), i.relay(), i.live())
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
