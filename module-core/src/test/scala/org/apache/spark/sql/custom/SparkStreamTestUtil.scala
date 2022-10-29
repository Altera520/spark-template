package org.apache.spark.sql.custom

import core.util.{SparkUtil, TimeUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ManualClock

/**
 * Stream unit test용 클래스
 */
// TODO: processingTime 이상하게 나옴, 수정해야함
class SparkStreamTestUtil(processingTime: Long = TimeUtil.getKstEpochMillis()) {
    val session = SparkUtil.buildSparkSession()
    import session.implicits._
    implicit val context = session.sqlContext
    val memoryStream = MemoryStream[String]

    // ManualClock는 private[spark]
    val processingTimeClock = new ManualClock(processingTime)
    val memorySink = new MemorySink

//    def produce[T, U](value: Map[T, U], processingTime: Long): Unit = {
//        this.produce(JsonUtil.toJson(value), processingTime)
//    }

    /**
     * @param value json format string
     * @return
     */
    def produce(value: String, processingTime: Long): Unit = {
        // processing time을 이동시킨다.
        processingTimeClock.advance(processingTime)
        memoryStream.addData(value)
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
