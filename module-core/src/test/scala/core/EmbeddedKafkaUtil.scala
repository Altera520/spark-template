package core

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer

object EmbeddedKafkaUtil extends EmbeddedKafka {
    implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
    implicit val serializer = new StringSerializer
    implicit val deserializer = new StringSerializer

}
