package prodcheck.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer;
import java.security.MessageDigest;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;

/** The README's two-level key: admin may do everything, reader only GET. */
public final class ApiKeyAuthorizer implements OutboxAdminAuthorizer {

    private final byte[] adminKey;
    private final byte[] readerKey;

    public ApiKeyAuthorizer(String adminKey, String readerKey) {
        if (adminKey.isBlank() || readerKey.isBlank()) {
            throw new IllegalArgumentException("outbox admin keys must not be blank");
        }
        this.adminKey = adminKey.getBytes(UTF_8);
        this.readerKey = readerKey.getBytes(UTF_8);
    }

    @Override
    public Decision authorize(HttpServerRequest request) {
        var header = request.headers().getFirst("X-Api-Key");
        var key = header == null ? new byte[0] : header.getBytes(UTF_8);
        if (MessageDigest.isEqual(key, adminKey)) {
            return OutboxAdminAuthorizer.allow("admin-key");
        }
        if (MessageDigest.isEqual(key, readerKey)) {
            return request.method().equals("GET") ? OutboxAdminAuthorizer.allow("reader-key")
                    : OutboxAdminAuthorizer.forbidden("the reader key only reads");
        }
        return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
    }
}
