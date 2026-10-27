package pl.example.kafkalab;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
@Profile("!tx")
@Configuration
public class KafkaErrorConfig {

  @Bean
  public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> jsonTemplate,
      @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {

    var bytesTemplate = new KafkaTemplate<String, byte[]>(new DefaultKafkaProducerFactory<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)));

    Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
    templates.put(byte[].class, bytesTemplate);
    templates.put(Object.class, jsonTemplate);

    var recoverer = new DeadLetterPublishingRecoverer(templates);   // → <topic>.DLT
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L)); // 3 ponowienia co 1 s
  }
}
