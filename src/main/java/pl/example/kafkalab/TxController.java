package pl.example.kafkalab;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpointy do ćwiczeń z transakcjami Kafki (Etap 6). Aktywne tylko w profilu "tx".
 */
@Profile("tx")
@RestController
@RequestMapping("/tx")
public class TxController {

  private final TxDemoService service;

  public TxController(TxDemoService service) {
    this.service = service;
  }

  /**
   * Wysyła jedno zamówienie na dwa topici w jednej transakcji (orders-audit i orders).
   * Z fail=true transakcja zostaje przerwana po pierwszej wiadomości.
   *
   * Przykład: POST /tx/orders?customerId=c-1&amount=10&fail=false
   */
  @PostMapping("/orders")
  public ResponseEntity<String> create(
      @RequestParam(defaultValue = "c-1") String customerId,
      @RequestParam(defaultValue = "10") BigDecimal amount,
      @RequestParam(defaultValue = "false") boolean fail) {

    var order = new OrderCreated(UUID.randomUUID().toString(), customerId, amount);
    try {
      service.sendBoth(order, fail);
    } catch (IllegalStateException e) {
      return ResponseEntity.status(HttpStatus.CONFLICT)
          .body("Transakcja przerwana dla " + order.orderId() + ": " + e.getMessage());
    }
    return ResponseEntity.accepted().body("Zatwierdzono " + order.orderId());
  }

  /**
   * Wysyła count zamówień (każde w osobnej transakcji), kwoty 10, 11, 12, 13...
   * Zamówienie z kwotą 13 powoduje błąd w OrderPipeline (zadanie 3).
   *
   * Przykład: POST /tx/orders/bulk?count=10
   */
  @PostMapping("/orders/bulk")
  public ResponseEntity<String> bulk(@RequestParam(defaultValue = "10") int count) {
    for (int i = 0; i < count; i++) {
      var customerId = "c-" + (i % 4 + 1);
      var order = new OrderCreated(UUID.randomUUID().toString(), customerId, BigDecimal.valueOf(10 + i));
      service.sendBoth(order, false);
    }
    return ResponseEntity.accepted().body("Wysłano " + count + " zamówień");
  }
}