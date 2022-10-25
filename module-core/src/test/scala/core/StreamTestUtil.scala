package core

import core.util.{JsonUtil, SparkUtil}
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.util.ManualClock

trait StreamTestUtil {
    val session = SparkUtil.buildSparkSession()
    import session.implicits._
    implicit val context = session.sqlContext
    val memoryStream = MemoryStream[String]

    def produce(value: Map[Symbol, Any]) = {
        memoryStream.addData(JsonUtil.toJson(value))
    }

    /**
     * @param value json format string
     * @return
     */
    def produce(value: String) = {
        memoryStream.addData(value)
    }

    def readStream(): DataFrame = {
        val schema = Encoders.product[T].schema
        memoryStream.toDF()
          .select(from_json($"value", schema) as "data")
          .select("data.*")
    }

//    def writeStream(df: DataFrame): StreamExecution = {
//        val processingTimeClock = new ManualClock(1L)
//        val memorySink = new MemorySinkV2
//        df.sparkSession
//          .streams
//          .startQuery(
//              userSpecifiedName = Some(""),
//              userSpecifiedCheckpointLocation = Some(""),
//              df = df,
//              sink = memorySink,
//              outputMode = OutputMode.Update(),
//              recoverFromCheckpointLocation = false,
//              triggerClock = processingTimeClock
//          )
//          .asInstanceOf[StreamingQueryWrapper]
//          .streamingQuery
//    }
}
