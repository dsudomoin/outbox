package prodcheck.accounts

import prodcheck.common.Controls
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.http.common.HttpMethod
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.server.common.HttpServerRequest
import ru.tinkoff.kora.http.server.common.HttpServerResponse
import ru.tinkoff.kora.http.server.common.annotation.HttpController

@Component
@HttpController
class AccountsController(private val controls: Controls) {

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
}
