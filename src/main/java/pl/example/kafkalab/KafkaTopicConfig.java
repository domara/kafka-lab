package pl.example.kafkalab;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ORDERS_TOPIC = "orders";
    public static final String ORDERS_AUDIT_TOPIC = "orders-audit";
    public static final String ORDERS_PROCESSED_TOPIC = "orders-processed";

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
  @Bean
  public NewTopic ordersAuditTopic() {
    return TopicBuilder.name(ORDERS_AUDIT_TOPIC)
        .partitions(3)
        .replicas(1)
        .build();
  }
  @Bean
  public NewTopic ordersProcessedTopic() {
    return TopicBuilder.name(ORDERS_PROCESSED_TOPIC)
        .partitions(3)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic ordersDlt() {
    return TopicBuilder.name("orders.DLT").partitions(3).replicas(1).build();
  }
}
