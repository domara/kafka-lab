package pl.example.kafkalab;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Profile("tx")
@Service
public class TxDemoService {
  private final KafkaTemplate<String, OrderCreated> template;
  public TxDemoService(KafkaTemplate<String, OrderCreated> template) { this.template = template; }

  public void sendBoth(OrderCreated o, boolean fail) {
    template.executeInTransaction(ops -> {
      ops.send("orders-audit", o.orderId(), o);
      if (fail) throw new IllegalStateException("przerwanie transakcji");
      ops.send("orders", o.customerId(), o);
      return null;
    });
  }
}
