package prodcheck.accounts

import com.google.protobuf.BytesValue
import io.github.dsudomoin.outbox.OutboxRecord
import prodcheck.common.Event
import prodcheck.common.Ledger
import prodcheck.common.Payload
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.security.MessageDigest
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset

enum class AccountEventResult {
    ACKNOWLEDGE,
    REQUEUE,
    WAIT,
}

/** {@code accounts.corruptPermanent}: the envelope will never parse. */
class CorruptEnvelope(cause: Exception) : RuntimeException(cause)

/** The proto3 number of the source field in the envelope, an unknown field of {@code BytesValue} here. */
const val SOURCE_FIELD = 2

@Component
class AccountOperation(
    private val db: JdbcConnectionFactory,
    private val config: AccountsConfig,
    prodcheck: ProdcheckConfig,
) {
    private val instance = prodcheck.instance()
    private val knownSources = config.knownSources().split(",").map { it.trim().toLong() }.toSet()

    /** A parse failure or a null payload throws: the handler answers Retry. */
    fun execute(record: OutboxRecord): AccountEventResult {
        val envelope = try {
            BytesValue.parseFrom(record.payload())
        } catch (e: Exception) {
            if (config.corruptPermanent()) throw CorruptEnvelope(e)
            throw e
        }
        val body = envelope.value.toByteArray()
        val event = Event(
            ROUTE, record.messageId(), record.topic(), record.keyAsString(),
            Payload.seq(body), Payload.tag(body), instance, 1,
        )
        db.inTx<Unit> { c: Connection -> Ledger.arrival(c, event) }
        db.inTx<Unit> { c: Connection -> checkSeen(c, record) }
        val source = envelope.unknownFields.getField(SOURCE_FIELD).varintList.firstOrNull() ?: 0L
        if (source !in knownSources) {
            // the service acknowledges a source it does not know: the event is gone, nothing applied
            return if (config.unknownSource() == "wait") AccountEventResult.WAIT else AccountEventResult.ACKNOWLEDGE
        }
        return db.inTx<AccountEventResult> { c: Connection -> business(c, event) }
    }

    private fun business(c: Connection, event: Event): AccountEventResult {
        val lockTimeout = config.lockTimeoutMs()
        if (lockTimeout > 0) {
            c.createStatement().use { it.execute("set local lock_timeout = $lockTimeout") }
        }
        val account = event.key()
        val subscribed = c.prepareStatement(
            "select id from accounts.subscription where account = ? and end_ts is null for update"
        ).use {
            it.setString(1, account)
            it.executeQuery().use { rs -> rs.next() }
        }
        if (!subscribed) {
            effect(c, event)
            return AccountEventResult.ACKNOWLEDGE
        }
        when (event.tag()) {
            "block" -> update(c, "insert into accounts.blocker (account) values (?) on conflict do nothing", account)
            // "wait+result": the same type as `wait`, the event that carries the result the older ones wait for
            "unblock", "wait+result" -> update(c, "delete from accounts.blocker where account = ?", account)
            // a block of its own, `<account>:ender`, keeps it parked; once through it ends the account's block
            "ender" -> {
                if (blocked(c, "$account:ender")) return AccountEventResult.REQUEUE
                update(c, "delete from accounts.blocker where account = ?", account)
            }
            // fails until `<account>:flaky` goes: a Retry head of the key, with the records behind it in the retry table
            "flaky" -> check(!blocked(c, "$account:flaky")) { "flaky while $account:flaky is there" }
            "wait" -> {
                // the useful change commits even when the event is requeued
                update(
                    c,
                    """insert into accounts.state (account, version) values (?, 1)
                       on conflict (account) do update set version = accounts.state.version + 1""",
                    account,
                )
                if (blocked(c, account)) return AccountEventResult.REQUEUE
            }
        }
        effect(c, event)
        return AccountEventResult.ACKNOWLEDGE
    }

    private fun blocked(c: Connection, account: String?): Boolean =
        c.prepareStatement("select 1 from accounts.blocker where account = ?").use {
            it.setString(1, account)
            it.executeQuery().use { rs -> rs.next() }
        }

    private fun update(c: Connection, sql: String, account: String?) {
        c.prepareStatement(sql).use {
            it.setString(1, account)
            it.executeUpdate()
        }
    }

    private fun effect(c: Connection, e: Event) {
        c.prepareStatement(
            """insert into accounts.pc_effect (sink, message_id, topic, key, seq, tag, contract)
               values (?, ?, ?, ?, ?, ?, ?) on conflict (sink, message_id) do nothing"""
        ).use {
            it.setString(1, e.sink())
            it.setObject(2, e.id())
            it.setString(3, e.topic())
            it.setString(4, e.key())
            it.setLong(5, e.seq())
            it.setString(6, e.tag())
            it.setInt(7, e.contract())
            it.executeUpdate()
        }
    }

    /** What the library promises to keep across retry and requeue: every later arrival is compared with the first. */
    private fun checkSeen(c: Connection, r: OutboxRecord) {
        val md5 = MessageDigest.getInstance("MD5").digest(r.payload()).joinToString("") { "%02x".format(it) }
        val headers = r.headers().joinToString("|") { h ->
            h.name() + "=" + (h.value()?.let { String(it, Charsets.UTF_8) } ?: "<null>")
        }
        val first = c.prepareStatement(
            """insert into accounts.seen
                 (message_id, id, created_at, topic, key, payload_md5, headers_text, max_requeues, max_attempts)
               values (?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict (message_id) do nothing"""
        ).use {
            it.setObject(1, r.messageId())
            it.setLong(2, r.id())
            it.setObject(3, OffsetDateTime.ofInstant(r.createdAt(), ZoneOffset.UTC))
            it.setString(4, r.topic())
            it.setString(5, r.keyAsString())
            it.setString(6, md5)
            it.setString(7, headers)
            it.setInt(8, r.requeues())
            it.setInt(9, r.attempts())
            it.executeUpdate() == 1
        }
        if (first) return

        val reasons = ArrayList<String>()
        c.prepareStatement(
            """select id, created_at, topic, key, payload_md5, headers_text, max_requeues, max_attempts
               from accounts.seen where message_id = ? for update"""
        ).use {
            it.setObject(1, r.messageId())
            it.executeQuery().use { rs ->
                rs.next()
                fun differs(name: String, was: Any?, now: Any?) {
                    if (was != now) reasons.add("$name was $was, now $now")
                }
                differs("id", rs.getLong(1), r.id())
                differs("created_at", rs.getObject(2, OffsetDateTime::class.java).toInstant(), r.createdAt())
                differs("topic", rs.getString(3), r.topic())
                differs("key", rs.getString(4), r.keyAsString())
                differs("payload_md5", rs.getString(5), md5)
                differs("headers", rs.getString(6), headers)
                if (r.requeues() < rs.getInt(7)) reasons.add("requeues went down from ${rs.getInt(7)} to ${r.requeues()}")
                if (r.attempts() < rs.getInt(8)) reasons.add("attempts went down from ${rs.getInt(8)} to ${r.attempts()}")
            }
        }
        for (reason in reasons) {
            c.prepareStatement("insert into accounts.mismatch (message_id, reason) values (?, ?)").use {
                it.setObject(1, r.messageId())
                it.setString(2, reason)
                it.executeUpdate()
            }
        }
        c.prepareStatement(
            """update accounts.seen set max_requeues = greatest(max_requeues, ?), max_attempts = greatest(max_attempts, ?)
               where message_id = ?"""
        ).use {
            it.setInt(1, r.requeues())
            it.setInt(2, r.attempts())
            it.setObject(3, r.messageId())
            it.executeUpdate()
        }
    }
}
