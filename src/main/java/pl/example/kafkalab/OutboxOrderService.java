package pl.example.kafkalab;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Zadanie 4 (Etap 6): zapis zamówienia i zdarzenia do tabeli outbox w JEDNEJ transakcji bazy.
 * Do Kafki nie wysyłamy nic bezpośrednio, robi to OutboxPublisher.
 */
@Profile("!tx")
@Service
public class OutboxOrderService {

  private final JdbcTemplate jdbc;
  private final ObjectMapper objectMapper;

  public OutboxOrderService(JdbcTemplate jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  @Transactional
  public void createOrder(OrderCreated order) {
    jdbc.update("INSERT INTO placed_orders(order_id, customer_id, amount) VALUES (?,?,?)",
        order.orderId(), order.customerId(), order.amount());

    jdbc.update("INSERT INTO outbox(event_key, topic, payload) VALUES (?,?,?)",
        order.customerId(), KafkaTopicConfig.ORDERS_TOPIC, toJson(order));
    // wyjątek w dowolnym miejscu wycofa OBA zapisy
  }

  private String toJson(OrderCreated order) {
    try {
      return objectMapper.writeValueAsString(order);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Nie można zserializować zamówienia", e);
    }
  }
}