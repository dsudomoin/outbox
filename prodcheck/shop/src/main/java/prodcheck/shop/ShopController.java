package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxRelay;
import java.util.Map;
import java.util.Set;
import prodcheck.common.Controls;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.http.common.HttpMethod;
import ru.tinkoff.kora.http.common.annotation.HttpRoute;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.annotation.HttpController;

@Component
@HttpController
public final class ShopController {

    private final Controls controls;
    private final DeadLetterPolicy hook;
    private final OutboxRelay relay;

    public ShopController(Controls controls, DeadLetterPolicy hook, OutboxRelay relay) {
        this.controls = controls;
        this.hook = hook;
        this.relay = relay;
    }

    @HttpRoute(method = HttpMethod.POST, path = "/drain")
    public HttpServerResponse drain() {
        return Controls.drain(relay);
    }

    @HttpRoute(method = HttpMethod.POST, path = "/load")
    public HttpServerResponse startLoad(HttpServerRequest r) {
        return controls.startLoad(r);
    }

    @HttpRoute(method = HttpMethod.DELETE, path = "/load")
    public HttpServerResponse stopLoad(HttpServerRequest r) {
        return controls.stopLoad(r);
    }

    @HttpRoute(method = HttpMethod.GET, path = "/stats")
    public HttpServerResponse stats() {
        return controls.stats();
    }

    @HttpRoute(method = HttpMethod.POST, path = "/tx/send")
    public HttpServerResponse txSend(HttpServerRequest r) {
        return controls.txSend(r);
    }

    @HttpRoute(method = HttpMethod.POST, path = "/tx/hold")
    public HttpServerResponse txHold(HttpServerRequest r) {
        return controls.txHold(r);
    }

    @HttpRoute(method = HttpMethod.POST, path = "/tx/outside")
    public HttpServerResponse txOutside(HttpServerRequest r) {
        return controls.txOutside(r);
    }

    @HttpRoute(method = HttpMethod.PUT, path = "/handler")
    public HttpServerResponse putHandler(HttpServerRequest r) {
        return controls.putHandler(r);
    }

    @HttpRoute(method = HttpMethod.GET, path = "/handler")
    public HttpServerResponse getHandler() {
        return controls.getHandler();
    }

    @HttpRoute(method = HttpMethod.PUT, path = "/hook")
    public HttpServerResponse putHook(HttpServerRequest r) {
        var mode = Controls.param(r, "mode");
        if (mode == null || !Set.of("ok", "throw", "hang", "bad-list").contains(mode)) {
            return Controls.json(400, Map.of("message", "mode must be ok, throw, hang or bad-list"));
        }
        hook.mode = mode;
        return Controls.json(200, Map.of("mode", mode));
    }
}
