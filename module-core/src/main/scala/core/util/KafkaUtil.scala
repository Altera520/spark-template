package core.util

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, OffsetSpec}
import org.apache.kafka.common.TopicPartition

import java.util.Properties

object KafkaUtil {
    def getAdminClient = {
        new KafkaAdminClientWrapper("bdp-dn1:9092,bdp-dn2:9092,bdp-dn3:9092")
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
     * timestamp에 대응하는 offset 반환
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
