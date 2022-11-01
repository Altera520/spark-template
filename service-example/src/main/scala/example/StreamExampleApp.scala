package example

import core.common.{Env, SparkBase}
import core.sink.HiveSink
import core.source.KafkaSource
import example.entity.Conf
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._

object StreamExampleApp extends SparkBase {
    implicit val hint = Env.buildConfigHint[Conf]()
    val conf = Env.getConfigOrThrow[Conf]()

    def readStream(topic: String, maxOffsetsPerTrigger: Int)(session: SparkSession) = {
        val df = KafkaSource.readStream(session, conf.kafkaUrl, topic, maxOffsetsPerTrigger)
        import session.implicits._

        df.select(get_json_object($"value".cast("string"), "$.payload") as "data")
    }

    def process(df: DataFrame) = {
        df
    }

    def writeStream(topic: String)(df: DataFrame) = {
        val dstTable = s"stage_$topic"
        df.writeStream
          .foreachBatch((df, batchId) => {
              HiveSink.write(dstTable)(df)
          })
          .start()
    }

    override def driver(session: SparkSession, args: Array[String]): Unit = {
        val topic = args(0)
        val maxOffsetsPerTrigger = if (args.length >= 2) args(1).toInt else 1000
        val pipe = readStream(topic, maxOffsetsPerTrigger) _ andThen process andThen writeStream(topic)
        val query = pipe(session)
        query.awaitTermination()
    }
}
