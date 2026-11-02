package prodcheck.courier

import io.github.dsudomoin.outbox.OutboxRecord
import prodcheck.common.Event
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.json.common.JsonReader
import ru.tinkoff.kora.json.common.JsonWriter
import ru.tinkoff.kora.json.common.annotation.Json
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

@Json
data class BatchItem(
    val id: UUID,
    val sink: String,
    val topic: String,
    val key: String?,
    val seq: Long,
    val tag: String,
    val contract: Int,
    val instance: String,
)

@Json
data class BatchResult(val id: UUID, val status: Int)

/** The target over the JDK client: `send` is interruptible, so an interrupt reaches the handler as InterruptedException. */
@Component
class TargetHttp(
    private val config: TargetHttpConfig,
    prodcheck: ProdcheckConfig,
    private val batchWriter: JsonWriter<List<BatchItem>>,
    private val batchReader: JsonReader<List<BatchResult>>,
) {
    private val instance = prodcheck.instance()
    private val http: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(config.requestTimeout())
        .build()

    fun post(sink: String, record: OutboxRecord): Int {
        val e = Event.of(record, sink, instance)
        val request = request("/events")
            .header("Idempotency-Key", e.id.toString())
            .header("X-Sink", e.sink)
            .header("X-Topic", e.topic)
            .header("X-Seq", e.seq.toString())
            .header("X-Tag", e.tag)
            .header("X-Contract", e.contract.toString())
            .header("X-Instance", e.instance)
        e.key?.let { request.header("X-Key", it) }
        return http.send(
            request.POST(HttpRequest.BodyPublishers.ofByteArray(record.payload() ?: ByteArray(0))).build(),
            HttpResponse.BodyHandlers.discarding(),
        ).statusCode()
    }

    /** The statuses of the records, in order; sink = topic. */
    fun postBatch(records: List<OutboxRecord>): List<Int> {
        val items = records.map {
            val e = Event.of(it, it.topic(), instance)
            BatchItem(e.id, e.sink, e.topic, e.key, e.seq, e.tag, e.contract, e.instance)
        }
        val response = http.send(
            request("/events/batch")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(batchWriter.toByteArray(items)))
                .build(),
            HttpResponse.BodyHandlers.ofByteArray(),
        )
        check(response.statusCode() == 200) { "target batch answered ${response.statusCode()}" }
        val results = batchReader.read(response.body())
        check(results != null && results.size == records.size) { "target batch answered ${results?.size} statuses" }
        return results.map { it.status }
    }

    private fun request(path: String): HttpRequest.Builder =
        HttpRequest.newBuilder(URI.create(config.url() + path)).timeout(config.requestTimeout())
}
