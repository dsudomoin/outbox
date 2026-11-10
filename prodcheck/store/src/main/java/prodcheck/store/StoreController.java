package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxRelay;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import prodcheck.common.Controls;

/** shop's control routes. */
@RestController
final class StoreController {

    private final Controls controls;
    private final DeadLetterPolicy hook;
    private final ObjectProvider<OutboxRelay> relay; // a prototype: the relay of the current start

    StoreController(Controls controls, DeadLetterPolicy hook, ObjectProvider<OutboxRelay> relay) {
        this.controls = controls;
        this.hook = hook;
        this.relay = relay;
    }

    @PostMapping("/drain")
    ResponseEntity<String> drain() {
        return http(Controls.drainReply(relay.getObject()));
    }

    @PostMapping("/load")
    ResponseEntity<String> startLoad(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.startLoad(q));
    }

    @DeleteMapping("/load")
    ResponseEntity<String> stopLoad(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.stopLoad(q));
    }

    @GetMapping("/stats")
    ResponseEntity<String> stats() {
        return http(controls.statsReply());
    }

    @PostMapping("/tx/send")
    ResponseEntity<String> txSend(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.txSend(q));
    }

    @PostMapping("/tx/hold")
    ResponseEntity<String> txHold(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.txHold(q));
    }

    @PostMapping("/tx/outside")
    ResponseEntity<String> txOutside(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.txOutside(q));
    }

    @PutMapping("/handler")
    ResponseEntity<String> putHandler(@RequestParam MultiValueMap<String, String> q) {
        return http(controls.putHandler(q));
    }

    @GetMapping("/handler")
    ResponseEntity<String> getHandler() {
        return http(controls.handlerReply());
    }

    @PutMapping("/hook")
    ResponseEntity<String> putHook(@RequestParam MultiValueMap<String, String> q) {
        var mode = Controls.param(q, "mode");
        if (mode == null || !Set.of("ok", "throw", "hang", "bad-list").contains(mode)) {
            return http(Controls.reply(400, Map.of("message", "mode must be ok, throw, hang or bad-list")));
        }
        hook.mode = mode;
        return http(Controls.reply(200, Map.of("mode", mode)));
    }

    private static ResponseEntity<String> http(Controls.Reply reply) {
        return ResponseEntity.status(reply.status()).contentType(MediaType.APPLICATION_JSON).body(reply.json());
    }
}
