package pl.example.kafkalab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.example.kafkalab.OrderCreated;

@Service
public class OrderService {
  private final JdbcTemplate jdbc;
  public OrderService(JdbcTemplate jdbc) { this.jdbc = jdbc; }
  private static final Logger log = LoggerFactory.getLogger(OrderService.class);
  @Transactional
  public void save(OrderCreated o) {
    try {
      jdbc.update("INSERT INTO processed_messages(message_id) VALUES (?)", o.orderId());
    } catch (DuplicateKeyException e) {
      log.info("Duplikat {} — pomijam", o.orderId());
      return;                              // już przetworzone
    }
    jdbc.update("INSERT INTO orders(order_id, customer_id, amount) VALUES (?,?,?)",
        o.orderId(), o.customerId(), o.amount());
  }
}