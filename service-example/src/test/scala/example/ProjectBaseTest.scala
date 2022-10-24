package example

import core.common.Env
import core.util.SparkUtil
import example.entity.Conf
import org.scalatest.funsuite.AnyFunSuite

class ProjectBaseTest extends AnyFunSuite {
    test("batch_example_app_logic_test") {
        // given
        val session = SparkUtil.buildSparkSession()
        import session.implicits._
        val data = Seq((1, 2))
        val df = data.toDF("col1", "col2")

        // when
        val processDF = BatchExampleApp.process(df)

        // then
        val actual = processDF.select($"col1").map(r => r.getInt(0)).collect().toList(0)
        val expected = data(0)._1 + 1
        assert(actual == expected)
    }

    test("conf_test") {
        // given & when
        import pureconfig.generic.auto._
        implicit val hint = Env.buildConfigHint[Conf]()
        val conf = Env.getConfigOrThrow[Conf]()

        // then
        assert(conf.tableName == "test2")
    }
}
