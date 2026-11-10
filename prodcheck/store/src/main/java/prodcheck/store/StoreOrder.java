package prodcheck.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

/** The business row each writer transaction saves through JPA, beside its message. */
@Entity
@Table(name = "store_order")
public class StoreOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private UUID messageId;

    @Column(nullable = false)
    private String topic;

    @Column(name = "order_key")
    private String key;

    private long seq;

    @Column(nullable = false)
    private String tag;

    protected StoreOrder() {}

    StoreOrder(UUID messageId, String topic, String key, long seq, String tag) {
        this.messageId = messageId;
        this.topic = topic;
        this.key = key;
        this.seq = seq;
        this.tag = tag;
    }
}
