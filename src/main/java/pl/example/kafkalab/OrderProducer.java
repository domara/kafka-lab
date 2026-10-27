package pl.example.kafkalab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
@Profile("!tx")
@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Klucz = customerId, wiec zamowienia jednego klienta trafiaja na te sama partycje (Etap 3). */
    public void send(OrderCreated order) {
        kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, order.customerId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Nie udalo sie wyslac zamowienia {}", order.orderId(), ex);
                    } else {
                        var meta = result.getRecordMetadata();
                        log.info("Wyslano {} -> partycja={}, offset={}",
                                order.orderId(), meta.partition(), meta.offset());
                    }
                });
    }
}
