package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import prodcheck.common.Event;
import prodcheck.common.HandlerControl;

/** shop's route that posts to the target over HTTP; the sink is the route's name. */
@Component
final class TargetRoute {

    private final RestClient client;
    private final HandlerControl control;
    private final String instance;

    TargetRoute(@Value("${prodcheck.target.url}") String url,
            @Value("${prodcheck.target.request-timeout}") Duration requestTimeout, HandlerControl control,
            @Value("${prodcheck.instance}") String instance) {
        var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(requestTimeout);
        this.client = RestClient.builder().baseUrl(url).requestFactory(factory).build();
        this.control = control;
        this.instance = instance;
    }

    void post(String route, OutboxRecord record) throws InterruptedException {
        control.apply(route, record.keyAsString());
        var e = Event.of(record, route, instance);
        var body = record.payload() == null ? new byte[0] : record.payload(); // a tombstone
        try {
            client.post().uri("/events")
                    .headers(h -> {
                        h.set("Idempotency-Key", e.id().toString());
                        h.set("X-Sink", e.sink());
                        h.set("X-Topic", e.topic());
                        if (e.key() != null) {
                            h.set("X-Key", e.key());
                        }
                        h.set("X-Seq", Long.toString(e.seq()));
                        h.set("X-Tag", e.tag());
                        h.set("X-Contract", Integer.toString(e.contract()));
                        h.set("X-Instance", e.instance());
                    })
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientResponseException ex) {
            var code = ex.getStatusCode().value();
            if (code >= 400 && code < 500 && code != 408 && code != 429) {
                throw new OutboxPermanentException(route + " rejected " + record.messageId(), ex);
            }
            throw ex; // 5xx, 408, 429: retried with backoff
        }
    }
}
