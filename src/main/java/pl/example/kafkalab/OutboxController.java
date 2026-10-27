package pl.example.kafkalab;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST /outbox/orders?customerId=c-1&amount=10
 * Zapisuje zamówienie i zdarzenie w bazie. Do Kafki poleci po chwili przez OutboxPublisher.
 */
@Profile("!tx")
@RestController
public class OutboxController {

  private final OutboxOrderService service;

  public OutboxController(OutboxOrderService service) {
    this.service = service;
  }

  @PostMapping("/outbox/orders")
  public ResponseEntity<OrderCreated> create(@RequestParam(defaultValue = "c-1") String customerId,
      @RequestParam(defaultValue = "10") BigDecimal amount) {
    var order = new OrderCreated(UUID.randomUUID().toString(), customerId, amount);
    service.createOrder(order);
    return ResponseEntity.accepted().body(order);
  }
}