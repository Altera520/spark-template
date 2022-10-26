package org.apache.spark.sql.custom

import core.util.{JsonUtil, SparkUtil}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.util.ManualClock

/**
 * Stream unit test용 클래스
 */
class StreamUnitTestUtil(session: SparkSession = SparkUtil.buildSparkSession(),
                         processingTime: Long = java.time.Instant.now().toEpochMilli) {
    import session.implicits._
    implicit val context = session.sqlContext
    val memoryStream = MemoryStream[String]

    // ManualClock는 private[spark]
    val processingTimeClock = new ManualClock(processingTime)
    val memorySink = new MemorySink

    def produce(value: Map[Symbol, Any]) = {
        memoryStream.addData(JsonUtil.toJson(value)).json()
    }

    /**
     * @param value json format string
     * @return
     */
    def produce(value: String, processingTime: Long = java.time.Instant.now().toEpochMilli) = {
        // processing time을 이동시킨다.
        processingTimeClock.advance(processingTime)
        memoryStream.addData(value).json()
    }

    def readStream(schema: StructType): DataFrame = {
        memoryStream.toDF()
          .select(from_json($"value", schema) as "data")
          .select("data.*")
    }

    def writeStream(df: DataFrame,
                    outputMode: OutputMode,
                    checkpointLocation: String = "/tmp/stream-unit-test"): StreamExecution = {
        df.sparkSession
          .streams
          // startQuery는 private[sql]
          .startQuery(
              userSpecifiedName = Some("stream-unit-test"),
              userSpecifiedCheckpointLocation = Some(checkpointLocation),
              extraOptions = Map[String, String](),
              df = df,
              sink = memorySink,
              outputMode = outputMode,
              recoverFromCheckpointLocation = false,
              triggerClock = processingTimeClock)
          .asInstanceOf[StreamingQueryWrapper]
          .streamingQuery
    }
}
