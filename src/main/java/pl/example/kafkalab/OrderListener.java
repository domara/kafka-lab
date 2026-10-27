package pl.example.kafkalab;

import java.math.BigDecimal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
@Profile("!tx")
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);
    private final OrderService orderService;
    public OrderListener(OrderService orderService) {
      this.orderService = orderService;
    }
    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC)
    public void onOrder(ConsumerRecord<String, OrderCreated> record) {
      if (record.value().amount().compareTo(new BigDecimal("13")) == 0) {
        throw new IllegalStateException("Symulowany błąd dla " + record.value().orderId());
      }
        orderService.save(record.value());
    }
  @DltHandler
  public void dlt(OrderCreated order, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    log.warn("DLT {}: {}", topic, order);
  }

  @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "audit-group")
  public void audit(ConsumerRecord<String, OrderCreated> record) {
    log.info("[AUDIT] key={} partycja={} offset={}", record.key(), record.partition(), record.offset());
  }
}
