package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** shop-n's HTTP routes: {@code payments} and {@code audit} raw, {@code orders} typed, reading v1 and v2. */
@Configuration(proxyBeanMethods = false)
class Handlers {

    /** A route of one topic that posts it to the target under the topic's name. */
    static final class Http implements OutboxTopicHandler {

        private final String topic;
        private final TargetRoute target;

        Http(String topic, TargetRoute target) {
            this.topic = topic;
            this.target = target;
        }

        @Override
        public Set<String> topics() {
            return Set.of(topic);
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            target.post(topic, record);
        }
    }

    static final class Orders extends TypedOutboxTopicHandler<OrderCreated> {

        private final TargetRoute target;

        Orders(OutboxDeserializer<OrderCreated> deserializer, TargetRoute target) {
            super(Set.of("orders"), deserializer);
            this.target = target;
        }

        @Override
        protected void handle(OrderCreated value, OutboxRecord record) throws Exception {
            target.post("orders", record);
        }
    }

    @Bean
    Http payments(TargetRoute target) {
        return new Http("payments", target);
    }

    @Bean
    Http audit(TargetRoute target) {
        return new Http("audit", target);
    }

    @Bean
    Orders orders(OutboxDeserializer<OrderCreated> deserializer, TargetRoute target) {
        return new Orders(deserializer, target);
    }
}
