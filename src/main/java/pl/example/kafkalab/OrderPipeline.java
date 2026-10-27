package pl.example.kafkalab;

import java.math.BigDecimal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Zadanie 3 (Etap 6): consume -> process -> produce w jednej transakcji Kafki.
 * Czyta z orders, wysyła wynik na orders-processed. Dla amount=13 rzuca wyjątek,
 * więc transakcja jest wycofywana (wynik niewidoczny dla read_committed, offset nie idzie do przodu).
 * Nie wołamy ack ręcznie: offset wejściowy jest zatwierdzany w tej samej transakcji.
 */
@Profile("tx")
@Component
public class OrderPipeline {

  private static final Logger log = LoggerFactory.getLogger(OrderPipeline.class);

  private final KafkaTemplate<String, OrderCreated> template;

  public OrderPipeline(KafkaTemplate<String, OrderCreated> template) {
    this.template = template;
  }

  @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "pipeline-group")
  public void process(ConsumerRecord<String, OrderCreated> record) {
    OrderCreated order = record.value();
    log.info("[PIPELINE] przetwarzam {} (partycja={}, offset={})",
        order.orderId(), record.partition(), record.offset());
    template.send(KafkaTopicConfig.ORDERS_PROCESSED_TOPIC, record.key(), order);
    if (order.amount().compareTo(new BigDecimal("13")) == 0) {
      throw new IllegalStateException("błąd przetwarzania dla " + order.orderId());
    }

     // w transakcji kontenera
    log.info("[PIPELINE] wysłano wynik {}", order.orderId());
  }
}