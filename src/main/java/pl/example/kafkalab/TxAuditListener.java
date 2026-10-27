package pl.example.kafkalab;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Zadanie 2 (Etap 6): konsument orders-audit z isolation.level=read_committed.
 * Wiadomości z przerwanych transakcji (fail=true) nie powinny się tu pojawić.
 */
@Profile("tx")
@Component
public class TxAuditListener {

  private static final Logger log = LoggerFactory.getLogger(TxAuditListener.class);

  @KafkaListener(topics = KafkaTopicConfig.ORDERS_AUDIT_TOPIC, groupId = "tx-audit")
  public void onAudit(ConsumerRecord<String, OrderCreated> record) {
    log.info("[TX-AUDIT] key={} partycja={} offset={} wartosc={}",
        record.key(), record.partition(), record.offset(), record.value());
  }
}