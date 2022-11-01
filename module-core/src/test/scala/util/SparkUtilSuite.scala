package util

import core.common.Env
import core.util.{JsonUtil, SparkUtil}
import org.apache.spark.sql.custom.SparkStreamTestUtil
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class SparkUtilSuite extends AnyFunSuite {
    test("tps_measurement_test") {
        val schema = StructType(
            Seq(StructField("url", StringType),
                StructField("timestamp", LongType))
        )
        val stream = new SparkStreamTestUtil()
        val df = stream.readStream(schema)
        val query = stream.writeStream(df, OutputMode.Update(), "/tmp/tps-test")

        SparkUtil.measurementTPS(stream.session, tps => {
            println("############## TPS ###############")
            println(tps)
        })

        // # batch 0
        stream.produce(JsonUtil.toJson(Map("url" -> "www.naver.com", "timestamp" -> 1000L)), 0L)
        query.processAllAvailable()
        stream.memorySink.clear()

        // # batch 1
        stream.produce(JsonUtil.toJson(Map("url" -> "www.naver.com/user", "timestamp" -> 1000L)), 0L)
        stream.produce(JsonUtil.toJson(Map("url" -> "www.naver.com", "timestamp" -> 1000L)), 0L)
        query.processAllAvailable()
    }

}
