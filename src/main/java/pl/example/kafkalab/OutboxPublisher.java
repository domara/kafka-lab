package pl.example.kafkalab;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Co sekundę bierze nieopublikowane wiersze z outbox i wysyła je do Kafki.
 * Gwarancja: co najmniej raz. Jeśli aplikacja padnie po wysłaniu, a przed oznaczeniem
 * wiersza jako opublikowany, wiadomość pójdzie drugi raz, więc konsument musi być idempotentny.
 */
@Profile("!tx")
@Component
public class OutboxPublisher {

  private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

  private record OutboxRow(long id, String key, String topic, String payload) { }

  private final JdbcTemplate jdbc;
  private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public OutboxPublisher(JdbcTemplate jdbc,
      KafkaTemplate<String, OrderCreated> kafkaTemplate,
      ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  @Scheduled(fixedDelay = 1000)
  public void publish() {
    List<OutboxRow> rows = jdbc.query(
        "SELECT id, event_key, topic, payload FROM outbox WHERE published = FALSE ORDER BY id",
        (rs, i) -> new OutboxRow(rs.getLong("id"), rs.getString("event_key"),
            rs.getString("topic"), rs.getString("payload")));

    for (OutboxRow row : rows) {
      try {
        OrderCreated event = objectMapper.readValue(row.payload(), OrderCreated.class);
        // czekamy na potwierdzenie brokera, dopiero potem oznaczamy wiersz
        kafkaTemplate.send(row.topic(), row.key(), event).get(10, TimeUnit.SECONDS);
        jdbc.update("UPDATE outbox SET published = TRUE WHERE id = ?", row.id());
        log.info("[OUTBOX] opublikowano wiersz {} -> {}", row.id(), row.topic());
      } catch (JsonProcessingException e) {
        log.error("[OUTBOX] wiersz {} ma niepoprawny payload, pomijam", row.id(), e);
      } catch (Exception e) {
        // zostawiamy wiersz jako nieopublikowany i przerywamy, żeby zachować kolejność
        log.warn("[OUTBOX] wysyłka wiersza {} nieudana ({}), spróbuję ponownie", row.id(), e.toString());
        return;
      }
    }
  }
}