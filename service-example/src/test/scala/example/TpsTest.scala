package example

import core.source.KafkaSource
import core.util.{SparkUtil, TimeUtil}
import org.scalatest.funsuite.AnyFunSuite

class TpsTest extends AnyFunSuite {
    test("tps_test") {
        val df = KafkaSource.read(SparkUtil.buildSparkSession(),
            "bdp-dn1:9092,bdp-dn2:9092,bdp-dn3:9092",
            "test-topic",
            TimeUtil.dateToTimestamp("20221021")
        ).selectExpr("cast(key as string)", "cast(value as string)")

        df.show(1, false)
    }
}
