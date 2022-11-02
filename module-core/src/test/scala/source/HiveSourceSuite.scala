package source

import core.source.HiveSource
import core.util.SparkUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HiveSourceSuite extends AnyFunSuite with BeforeAndAfterAll {

    private var session: SparkSession = _
    private var dstTable: String = _

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        val data = Seq(
            ("www.naver.com", "20221031"),
            ("www.daum.com", "20221101"),
        )
        session = SparkUtil.buildSparkSession()
        dstTable = "test_table"
        val df = session.createDataFrame(data).toDF("url", "p_dt")
        session.sql(s"drop table $dstTable")
        df.write.saveAsTable(dstTable)
    }

    test("hive_table_read_stream") {
        val stream = HiveSource.readStream(session, dstTable)

        val query = stream.writeStream
          .format("console")
          .start()

        query.processAllAvailable()
    }
}
