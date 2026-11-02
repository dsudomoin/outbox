package io.github.dsudomoin.outbox.admin.spring;

import io.github.dsudomoin.outbox.admin.spring.model.Error;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedRuntimeException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * The failures of {@link OutboxAdminController}, and of no other controller, as JSON {@code Error} bodies: a call's
 * {@link IllegalArgumentException} 400, its {@link IllegalStateException} 409, a request MVC could not read 400, any
 * other exception MVC gives a status (a missing parameter) that status, and anything else 500 with the failure's
 * message, for the authenticated operator; a 5xx after one WARN with the exception. First in order, so an app's global
 * advice, or Boot's problem details, do not answer for this controller.
 */
@RestControllerAdvice(assignableTypes = OutboxAdminController.class)
@Order(Ordered.HIGHEST_PRECEDENCE)
class OutboxAdminErrors {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminErrors.class);

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Error> refused(IllegalArgumentException e) {
        return error(HttpStatus.BAD_REQUEST.value(), message(e));
    }

    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Error> conflict(IllegalStateException e) {
        return error(HttpStatus.CONFLICT.value(), message(e));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<Error> badParameter(MethodArgumentTypeMismatchException e) {
        return error(HttpStatus.BAD_REQUEST.value(),
            "Query parameter '" + e.getName() + "' has invalid value: " + e.getValue());
    }

    /** The parser's own words; Spring's for a missing body would name this controller's method. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<Error> unreadable(HttpMessageNotReadableException e) {
        return error(HttpStatus.BAD_REQUEST.value(),
            e.getCause() == null ? "a request body is required" : message(e.getMostSpecificCause()));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Error> failed(Exception e, HttpServletRequest request) {
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
}
