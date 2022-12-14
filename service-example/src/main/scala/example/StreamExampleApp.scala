package example

import core.common.{Env, SparkBase}
import core.sink.HiveSink
import core.source.KafkaSource
import core.util.TimeUtil
import example.entity.Conf
import org.apache.spark.sql.functions.{get_json_object, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._

object StreamExampleApp extends SparkBase {
    implicit val hint = Env.buildConfigHint[Conf]()
    val conf = Env.getConfigOrThrow[Conf]()

    def readStream(topic: String, maxOffsetsPerTrigger: Int, startingOffsets: String)(session: SparkSession) = {
        val df = KafkaSource.readStream(session, conf.kafkaUrl, topic, maxOffsetsPerTrigger, startingOffsets)
        import session.implicits._

        // debezium-mysql connector로 부터 수집된 데이터에서 payload만 추출
        df.select(
            get_json_object($"value".cast("string"), "$.payload") as "data",
            lit(TimeUtil.epochMillisToDateString(java.time.Instant.now().toEpochMilli, "yyyyMMdd")) as "p_dt"
        )
    }

    def process(df: DataFrame) = {
        df
    }

    def writeStream(dstTable: String)(df: DataFrame) = {
        HiveSink.writeStream(
            df,
            dstTable,
            Trigger.ProcessingTime("1 minute")
        )
    }

    override def driver(session: SparkSession, args: Array[String]): Unit = {
        val topic = args(0)
        val dstTable = args(1)
        val startingOffsets = args(2)
        val maxOffsetsPerTrigger = if (args.length >= 4) args(3).toInt else 1000
        val pipe = readStream(topic, maxOffsetsPerTrigger, startingOffsets) _ andThen process andThen writeStream(dstTable)
        val query = pipe(session)
        query.awaitTermination()
    }
}
