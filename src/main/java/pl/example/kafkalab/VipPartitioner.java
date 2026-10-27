package pl.example.kafkalab;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

public class VipPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes,
      Object value, byte[] valueBytes, Cluster cluster) {
    int numPartitions = cluster.partitionsForTopic(topic).size();

    // dla innych topiców (retry, DLT) albo topiców z jedną partycją: zwykłe haszowanie po kluczu
    if (!KafkaTopicConfig.ORDERS_TOPIC.equals(topic) || numPartitions < 2) {
      if (keyBytes == null || numPartitions < 2) return 0;
      return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    if (key != null && key.toString().startsWith("vip-")) {
      return 0;
    }
    if (keyBytes == null) {
      return ThreadLocalRandom.current().nextInt(1, numPartitions);
    }
    return 1 + Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - 1);
  }
  @Override public void close() { }
  @Override public void configure(Map<String, ?> configs) { }
}