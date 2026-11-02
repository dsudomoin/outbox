package prodcheck.shop;

import jakarta.annotation.Nullable;
import ru.tinkoff.kora.http.client.common.annotation.HttpClient;
import ru.tinkoff.kora.http.common.HttpMethod;
import ru.tinkoff.kora.http.common.annotation.Header;
import ru.tinkoff.kora.http.common.annotation.HttpRoute;

@HttpClient(configPath = "httpClient.target")
public interface TargetClient {

    @HttpRoute(method = HttpMethod.POST, path = "/events")
    void post(
            @Header("Idempotency-Key") String id,
            @Header("X-Sink") String sink,
            @Header("X-Topic") String topic,
            @Nullable @Header("X-Key") String key,
            @Header("X-Seq") String seq,
            @Header("X-Tag") String tag,
            @Header("X-Contract") String contract,
            @Header("X-Instance") String instance,
            byte[] body);
}
