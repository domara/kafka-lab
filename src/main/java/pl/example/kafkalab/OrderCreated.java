package pl.example.kafkalab;

import java.math.BigDecimal;

public record OrderCreated(String orderId, String customerId, BigDecimal amount) {
}
