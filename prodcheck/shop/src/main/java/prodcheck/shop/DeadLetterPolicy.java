package prodcheck.shop;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import prodcheck.common.Payload;

/** {@code prodcheck.hook = policy}: decides by the payload's tag. {@code PUT /hook} makes it misbehave. */
public final class DeadLetterPolicy implements OutboxDeadLetterHandler {

    private final boolean active;
    volatile String mode = "ok";

    DeadLetterPolicy(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return active;
    }

    @Override
    public List<DeadLetterDecision> handle(List<DeadLetter> letters) throws Exception {
        switch (mode) {
            case "throw" -> throw new RuntimeException("hook mode throw");
            case "hang" -> Thread.sleep(3_600_000);
            case "bad-list" -> {
                return List.of();
            }
            default -> {}
        }
        var decisions = new ArrayList<DeadLetterDecision>(letters.size());
        for (var letter : letters) {
            decisions.add(switch (Payload.tag(letter.payload())) {
                case "hook-discard" -> DeadLetterDecision.discard();
                case "hook-redrive" -> letter.redrives() < 1 ? DeadLetterDecision.redrive() : DeadLetterDecision.keep();
                case "hook-redrive-at" -> letter.redrives() < 1
                        ? DeadLetterDecision.redriveAt(Instant.now().plusSeconds(3))
                        : DeadLetterDecision.keep();
                default -> DeadLetterDecision.keep();
            });
        }
        return decisions;
    }
}
