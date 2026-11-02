package prodcheck.accounts

import com.google.protobuf.BytesValue
import com.google.protobuf.ByteString
import com.google.protobuf.UnknownFieldSet
import io.github.dsudomoin.outbox.Outbox
import io.github.dsudomoin.outbox.OutboxHeader
import io.github.dsudomoin.outbox.OutboxMessage
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox
import prodcheck.common.Payload
import prodcheck.common.Sender
import java.util.UUID

const val UNKNOWN_SOURCE = 99L

/** Through `ProtobufOutbox` with the envelope and headers a stream consumer would write. */
class AccountsSender(
    private val outbox: Outbox,
    private val producer: ProtobufOutbox,
    private val table: String,
) : Sender {

    override fun table(): String = table

    override fun topics(): List<String> = listOf(ROUTE)

    override fun outbox(): Outbox = outbox

    override fun send(topic: String, key: String?, seq: Long, tag: String): UUID =
        producer.send(topic, key, envelope(seq, tag), headers(seq))


    override fun message(topic: String, key: String?, seq: Long, tag: String): OutboxMessage =
        OutboxMessage(topic, key?.toByteArray(Charsets.UTF_8), envelope(seq, tag).toByteArray(), headers(seq))

    /** Tag `unknown-source`: a source number no release reads unless `accounts.knownSources` names it. */
    private fun envelope(seq: Long, tag: String): BytesValue {
        val builder = BytesValue.newBuilder().setValue(ByteString.copyFrom(Payload.of(seq, tag)))
        if (tag == "unknown-source") {
            builder.setUnknownFields(
                UnknownFieldSet.newBuilder()
                    .addField(SOURCE_FIELD, UnknownFieldSet.Field.newBuilder().addVarint(UNKNOWN_SOURCE).build())
                    .build()
            )
        }
        return builder.build()
    }

    private fun headers(seq: Long): List<OutboxHeader> {
        val source = if (seq % 2 == 0L) "snapshot" else "charge"
        val headers = mapOf(
            "source" to source,
            "topic" to "$source-stream",
            "partition" to (seq % 8).toString(),
            "offset" to seq.toString(),
        ) + if (source == "snapshot") mapOf("snapshot-consumer" to "accounts") else emptyMap()
        return headers.map { (name, value) -> OutboxHeader(name, value.toByteArray(Charsets.UTF_8)) }
    }
}
