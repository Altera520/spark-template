package example

import core.util.JsonUtil
import org.apache.spark.sql.custom.StreamUnitTestUtil
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite


class StreamUnitTestUtilExample extends AnyFunSuite {

    test("basic_usage") {
        /*
        This query does not support recovering from checkpoint location.
        테스트 재시작시, checkpoint location을 삭제하고 실행해야한다.
        (default: /tmp/stream-unit-test)
         */
        val schema = StructType(
            Seq(StructField("url", StringType),
                StructField("timestamp", LongType))
        )
        val stream = new StreamUnitTestUtil()
        val df = stream.readStream(schema)
        val query = stream.writeStream(df, OutputMode.Update())

        // # batch 0
        stream.produce(JsonUtil.toJson(Map("url" -> "www.naver.com", "timestamp" -> 1000L)))
        query.processAllAvailable() // 소스에서 사용 가능한 모든 데이터가 처리되고 싱크에 커밋될 때까지 await thread (blocking)
        stream.memorySink.allData.foreach(println)
        // batch마다 clear, clear 안하면 다음 batch에 현재 output이 포함
        stream.memorySink.clear()

        // # batch 1
        // 시간이 5초가 지난후에 데이터가 produce되었다고 가정
        stream.produce(JsonUtil.toJson(Map("url" -> "www.naver.com/user", "timestamp" -> 1000L)), 5000L)
        query.processAllAvailable()
        stream.memorySink.allData.foreach(println)
        stream.memorySink.clear()
    }

    test("test_util_with_tps_measurement") {

    }
}
