package util

import core.util.{JsonUtil, KafkaAdminClientWrapper, KafkaUtil}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

class KafkaUtilSuite extends AnyFunSuite with EmbeddedKafka {
    val kafkaPort = 9092
    val zooKeeperPort = 2181
    implicit val config = EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)
    implicit val serializer: Serializer[String] = new StringSerializer
    implicit val deserializer: Deserializer[String] = new StringDeserializer

    /**
     * Embedded Kafka에 produce
     * @param topic 토픽명
     * @param records (key, value, timestamp)
     * @param partition -1이면 RR방식으로 파티션 분배
     */
    def produce(topic: String, records: Iterable[(String, String, Long)], partition: Int = -1) = {
        val partitioner = if (partition == -1) {
            // Round Robin partitioner
            val numPartition = KafkaUtil.getAdminClient(s"localhost:$kafkaPort")
              .getPartitionCount(topic)
            (_: Int) % numPartition
        } else {
            (_: Int) => partition
        }

        EmbeddedKafka.withProducer[String, String, Unit]((producer: KafkaProducer[String, String]) => {
            records.zipWithIndex.foreach { case ((key, value, timestamp), index) =>
                val record = new ProducerRecord[String, String](topic, partitioner(index), timestamp, key, value)
                producer.send(record)
            }
        })
    }

    /**
     * Embedded Kafka에 consume
     * @param topic 토픽명
     * @param pollSeconds poll하기까지 대기시간
     * @return
     */
    def consume(topic: String, pollSeconds: Int = 1) = {
        EmbeddedKafka.withConsumer[String, String, Iterable[ConsumerRecord[String, String]]]((consumer: KafkaConsumer[String, String]) => {
            import scala.jdk.CollectionConverters._
            consumer.subscribe(Collections.singletonList(topic))
            consumer.poll(java.time.Duration.ofMillis(1000L * pollSeconds)).asScala
        })
    }

    test("get_topic_partition_count") {
        val partitionCount = 3
        withRunningKafka {
            createCustomTopic(
                topic = "test-topic",
                partitions = partitionCount,
                replicationFactor = 1)

            val client = new KafkaAdminClientWrapper(s"localhost:$kafkaPort")
            assert(client.getPartitionCount("test-topic") == partitionCount)
        }
    }

    test("get_offset_by_timestamp") {
        withRunningKafka {
            createCustomTopic(
                topic = "test-topic",
                partitions = 1,
                replicationFactor = 1)

            val data = Seq(
                ("", JsonUtil.toJson(Map("url" -> "www.naver.com")), 1L),       // offset: 0
                ("", JsonUtil.toJson(Map("url" -> "www.naver.com/news")), 2L),  // offset: 1
                ("", JsonUtil.toJson(Map("url" -> "www.naver.com/blog")), 3L),  // offset: 2
            )
            val client = new KafkaAdminClientWrapper(s"localhost:$kafkaPort")

            produce("test-topic", data)
            val (_, Some(offset)) = client.getOffsetByTimestamp("test-topic", 3L).toList(0)

            assert(offset == 2)
        }
    }
}
