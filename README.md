# kafka-lab

Szkielet do nauki Kafki w Spring Boot (Java 17, Gradle, Spring Boot 3.3).

## Start

1. Uruchom Kafke (KRaft) i Kafka UI:

   ```
   docker compose up -d
   ```

   Kafka UI: http://localhost:8090 (klaster `local`).

2. Uruchom aplikacje:

   ```
   gradle bootRun
   ```

   (albo dodaj Gradle wrapper: `gradle wrapper`, a potem `./gradlew bootRun`)

3. Wyslij zamowienie:

   ```
   curl -X POST localhost:8080/orders \
     -H 'Content-Type: application/json' \
     -d '{"customerId":"c-1","amount":49.99}'
   ```

   W logach zobaczysz wyslanie (partycja, offset) i odbior przez `@KafkaListener`.

## Narzedzia CLI z Etapu 1

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic orders --property parse.key=true --property key.separator=:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic orders --group cli-group --from-beginning
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group orders-group
```

Uwaga: kontener `kafka` nasluchuje wewnatrz na `19092` (listener INTERNAL), a z hosta (aplikacja Spring) na `localhost:9092`.

## Dalsze kroki

Plan etapow 1-8 i projekt koncowy: dokument z checklista (w czacie).
