package core.source

import core.util.{JsonUtil, KafkaUtil}
import org.apache.spark.sql.SparkSession

object KafkaSource {

    /**
     * kafka를 stream 방식으로 read
     * @param session
     * @param bootstrapServers
     * @param topic
     * @param options
     * @return
     */
    def readStream(session: SparkSession,
                   bootstrapServers: String,
                   topic: String,
                   maxOffsetsPerTrigger: Int,
                   options: Map[String, String] = Map()) = {
        val df = session.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("subscribe", topic)
          .option("startingOffsets", "latest")
          .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
          .options(options)
          .load()
        df
    }

    /**
     * kafka를 batch 방식으로 read
     * @param session
     * @param bootstrapServers
     * @param topic
     * @param options
     * @return
     */
    def read(session: SparkSession,
             bootstrapServers: String,
             topic: String,
             startingTimestamp: Long,
             endingTimestamp: Long = java.time.Instant.now.toEpochMilli,
             options: Map[String, String] = Map()) = {

        def mkJson(timestamp: Long, `else`: Int) = {
            val values = KafkaUtil.getAdminClient(bootstrapServers).getOffsetByTimestamp(topic, timestamp).map {
                case (partition, offset) => partition.toString -> offset.getOrElse(`else`)
            }
            JsonUtil.toJson(Map("values" -> values.toMap))
        }

        val df = session.read
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("subscribe", topic)
          .option("startingOffsets", mkJson(startingTimestamp, -2))
          .option("endingOffsets", mkJson(endingTimestamp, -1))
          .options(options)
          .load()
        df
    }
}
