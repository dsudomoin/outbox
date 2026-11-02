package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxRelay
import prodcheck.common.Controls
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.http.common.HttpMethod
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.server.common.HttpServerRequest
import ru.tinkoff.kora.http.server.common.HttpServerResponse
import ru.tinkoff.kora.http.server.common.annotation.HttpController

@Component
@HttpController
class CourierController(private val controls: Controls, private val second: SecondOutbox, private val relay: OutboxRelay) {

    @HttpRoute(method = HttpMethod.POST, path = "/drain")
    fun drain(): HttpServerResponse = Controls.drain(relay)

    @HttpRoute(method = HttpMethod.POST, path = "/load")
    fun startLoad(r: HttpServerRequest): HttpServerResponse = controls.startLoad(r)

    @HttpRoute(method = HttpMethod.DELETE, path = "/load")
    fun stopLoad(r: HttpServerRequest): HttpServerResponse = controls.stopLoad(r)

    @HttpRoute(method = HttpMethod.GET, path = "/stats")
    fun stats(): HttpServerResponse = controls.stats()

    @HttpRoute(method = HttpMethod.POST, path = "/tx/send")
    fun txSend(r: HttpServerRequest): HttpServerResponse = controls.txSend(r)

    @HttpRoute(method = HttpMethod.POST, path = "/tx/hold")
    fun txHold(r: HttpServerRequest): HttpServerResponse = controls.txHold(r)

    @HttpRoute(method = HttpMethod.POST, path = "/tx/outside")
    fun txOutside(r: HttpServerRequest): HttpServerResponse = controls.txOutside(r)

    @HttpRoute(method = HttpMethod.PUT, path = "/handler")
    fun putHandler(r: HttpServerRequest): HttpServerResponse = controls.putHandler(r)

    @HttpRoute(method = HttpMethod.GET, path = "/handler")
    fun getHandler(): HttpServerResponse = controls.getHandler()

    @HttpRoute(method = HttpMethod.GET, path = "/second/status")
    fun secondStatus(): HttpServerResponse {
        val s = second.runtime.admin().status()
        return Controls.json(
            200,
            mapOf(
                "table" to second.table,
                "halted" to s.halted(),
                "lagSeconds" to s.lagSeconds(),
                "retrySize" to s.retrySize(),
                "parkedSize" to s.parkedSize(),
                "waitingSize" to s.waitingSize(),
                "waitingOldestSeconds" to s.waitingOldestSeconds(),
                "deadLetters" to s.deadLetters().map {
                    mapOf("topic" to it.topic(), "status" to it.status().name, "count" to it.count())
                },
                "orphans" to s.orphans().map { it.table() },
                "tailOverflow" to s.tailOverflow(),
                "premade" to s.premade(),
                "xidsLeft" to s.xidsLeft(),
                "maintenanceSinceSuccessSeconds" to s.maintenanceSinceSuccessSeconds(),
                "shards" to s.shards(),
                "instances" to s.instances().map {
                    mapOf("id" to it.id(), "live" to it.live(), "relay" to it.relay(), "lastSeen" to it.lastSeen().toString())
                },
            ),
        )
    }
}
