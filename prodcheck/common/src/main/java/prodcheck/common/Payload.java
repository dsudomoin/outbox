package prodcheck.common;

import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/** The message body: {@code {"seq":N,"tag":"...","pad":"xxx"}}, about 300 bytes. Tags hold no quotes. */
public final class Payload {

    private static final int SIZE = 300;

    private Payload() {}

    public static String pad(long seq, String tag) {
        return "x".repeat(Math.max(0, SIZE - 28 - tag.length() - Long.toString(seq).length()));
    }

    public static byte[] of(long seq, String tag) {
        return ("{\"seq\":" + seq + ",\"tag\":\"" + tag + "\",\"pad\":\"" + pad(seq, tag) + "\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    /** 0 for a tombstone or a payload that is not ours ({@code /tx/send?raw=}). */
    public static long seq(@Nullable byte[] payload) {
        var json = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
        var at = json.indexOf("\"seq\":");
        if (at < 0) {
            return 0;
        }
        var from = at + 6;
        var to = from;
        while (to < json.length() && Character.isDigit(json.charAt(to))) {
            to++;
        }
        return Long.parseLong(json.substring(from, to));
    }

    /** Empty for a tombstone or a payload that is not ours. */
    public static String tag(@Nullable byte[] payload) {
        var json = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
        var at = json.indexOf("\"tag\":\"");
        if (at < 0) {
            return "";
        }
        var from = at + 7;
        return json.substring(from, json.indexOf('"', from));
    }

    /** Tags the sink that decides treats as a permanent failure until told otherwise ({@code poisonFixed}). */
    public static boolean poisoned(String tag) {
        return tag.equals("poison") || tag.startsWith("hook-");
    }
}
