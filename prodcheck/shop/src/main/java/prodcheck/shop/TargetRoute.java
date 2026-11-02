package prodcheck.shop;

import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.http.client.common.HttpClientResponseException;

/** A route that posts to the target over HTTP; the sink is the route's name. */
@Component
public final class TargetRoute {

    private final TargetClient client;
    private final HandlerControl control;
    private final String instance;

    public TargetRoute(TargetClient client, HandlerControl control, ShopApp.ProdcheckConfig config) {
        this.client = client;
        this.control = control;
        this.instance = config.instance();
    }

    void post(String route, OutboxRecord record) throws InterruptedException {
        control.apply(route, record.keyAsString());
        var e = Event.of(record, route, instance);
        var body = record.payload() == null ? new byte[0] : record.payload(); // a tombstone
        try {
            client.post(e.id().toString(), e.sink(), e.topic(), e.key(), Long.toString(e.seq()), e.tag(),
                    Integer.toString(e.contract()), e.instance(), body);
        } catch (HttpClientResponseException ex) {
            if (ex.getCode() >= 400 && ex.getCode() < 500 && ex.getCode() != 408 && ex.getCode() != 429) {
                throw new OutboxPermanentException(route + " rejected " + record.messageId(), ex);
            }
            throw ex; // 5xx, 408, 429: retried with backoff
        }
    }
}
