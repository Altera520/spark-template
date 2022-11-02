package core.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, OffsetSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.collection.mutable

object KafkaUtil {
    private val adminClientPool = mutable.HashMap[String, KafkaAdminClientWrapper]()
    private val producerPool = mutable.HashMap[(String, String), KafkaProducerImpl]()

    /**
     * kafka admin client 반환
     * @param bootstrapServers
     * @return
     */
    def getAdminClient(bootstrapServers: String) = {
        adminClientPool.getOrElseUpdate(bootstrapServers, new KafkaAdminClientWrapper(bootstrapServers))
    }

    /**
     * kafka producer 반환
     * @param bootstrapServers
     * @param topic
     * @return
     */
    def getProducer(bootstrapServers: String, topic: String) = {
        val key = (bootstrapServers, topic)
        producerPool.getOrElseUpdate(key, new KafkaProducerImpl(bootstrapServers, topic))
    }
}

class KafkaAdminClientWrapper(bootstrapServers: String) {
    private val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    private val client = AdminClient.create(props)

    def getPartitionCount(topic: String) = {
        import scala.jdk.CollectionConverters._
        val topicDescription = client.describeTopics(List(topic).asJavaCollection)
          .values()
          .get(topic)

        topicDescription.get().partitions().size()
    }

    /**
     * topic의 timestamp에 대응하는 offset 반환
     * @param topic
     * @param timestamp
     * @return Iterable[(partition, offset)]
     */
    def getOffsetByTimestamp(topic: String, timestamp: Long) = {
        import scala.jdk.CollectionConverters._
        val args = (0 until getPartitionCount(topic) map (new TopicPartition(topic, _) -> OffsetSpec.forTimestamp(timestamp))).toMap

        client.listOffsets(args.asJava)
          .all
          .get
          .values
          .asScala
          .zipWithIndex
          .map { item =>
              val offset = item._1.offset match {
                  case -1 => None
                  case v => Some(v)
              }
              item._2 -> offset
          }
    }
}

class KafkaProducerImpl(bootstrapServers: String, topic: String) {
    private val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    private val producer = new KafkaProducer[String, String](props)
    private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    def produce(key: String, value: Object) = {
        producer.send(new ProducerRecord[String, String](this.topic, key, mapper.writeValueAsString(value)))
    }
}
