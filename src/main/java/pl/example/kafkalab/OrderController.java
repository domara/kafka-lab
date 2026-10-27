package pl.example.kafkalab;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@Profile("!tx")
@RestController
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    public record CreateOrderRequest(String customerId, BigDecimal amount) {
    }

    @PostMapping("/orders")
    public ResponseEntity<OrderCreated> create(@RequestBody CreateOrderRequest request) {
        var order = new OrderCreated(UUID.randomUUID().toString(), request.customerId(), request.amount());
        producer.send(order);
        return ResponseEntity.accepted().body(order);
    }

  @PostMapping("/orders/bulk")
  public String bulk(@RequestParam(defaultValue = "10") int count) {
    for (int i = 0; i < count; i++) {
      var customerId = "c-" + (i % 4 + 1);   // c-1..c-4, część kluczy się powtarza
      if(i%5==0) customerId = "vip-" + customerId;
      producer.send(new OrderCreated(UUID.randomUUID().toString(), customerId, BigDecimal.valueOf(10 + i)));
    }
    return "wysłano " + count;
  }
}
