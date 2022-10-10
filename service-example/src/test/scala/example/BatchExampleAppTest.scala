package example

import core.util.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class BatchExampleAppTest extends AnyFunSuite {
    test("process_logic_test") {
        // given
        val session = SparkUtil.buildSparkSession()
        import session.implicits._
        val data = Seq((1, 2))
        val df = data.toDF("col1", "col2")

        // when
        val processDF = BatchExampleApp.process(df)

        // then
        val actual = processDF.select("col1").map(r => r.getInt(0)).collect().toList(0)
        val expected = data(0)._1 + 1
        assert(actual == expected)
    }
}
