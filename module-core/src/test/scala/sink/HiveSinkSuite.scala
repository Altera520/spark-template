package sink

import core.sink.HiveSink
import core.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class HiveSinkSuite extends AnyFunSuite with BeforeAndAfterEach {

    // allocate default value
    private var session: SparkSession = _
    private var sampleDF: DataFrame = _
    private var data: Seq[(String, String, String, String)] = _
    private var nonPartitionedTable: String = _
    private var partitionedTable: String = _

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        nonPartitionedTable = "latest_access_url"
        partitionedTable = "latest_access_url_p"

        data = Seq(
            ("a", "www.naver.com", "2022-10-31 13:50:10", "20221031"),       // row 0
            ("a", "www.naver.com/blog", "2022-10-31 13:54:10", "20221031"),  // row 1, this row is latest in 20221031
            ("b", "www.zum.com", "2022-10-31 14:50:10", "20221031"),         // row 2
            ("b", "www.daum.com", "2022-10-31 13:50:10", "20221030")         // row 3
        )

        session = SparkUtil.buildSparkSession()
        sampleDF = session.createDataFrame(data).toDF("user_id", "url", "modified_dt", "p_dt")

        session.sql(s"drop table if exists $nonPartitionedTable")
        session.sql(
            s"""
               |create table if not exists $nonPartitionedTable(
               |    user_id string,
               |    url string,
               |    modified_dt string,
               |    p_dt string
               |)
               |""".stripMargin)

        session.sql(s"drop table if exists $partitionedTable")
        session.sql(
            s"""
               |create table if not exists $partitionedTable(
               |    user_id string,
               |    url string,
               |    modified_dt string
               |) partitioned by (p_dt string)
               |""".stripMargin)
    }


    test("write_to_hive_table") {
        HiveSink.write(nonPartitionedTable)(sampleDF)
        assert(session.sql(s"select * from $nonPartitionedTable").count() == data.length)
    }

    test("write_to_hive_table_with_partition") {
        HiveSink.write(partitionedTable, partitionColumn = Some("p_dt"))(sampleDF)
        assert(session.sql(s"select * from $partitionedTable").count() == data.length)
    }

    test("write_to_hive_table_and_then_distinct") {
        HiveSink.write(
            nonPartitionedTable,
            timestampColumn = Some("modified_dt"),
            primaryColumns = Some(Seq("user_id", "p_dt")))(sampleDF)
        assert(session.sql(s"select * from $nonPartitionedTable").count() == data.groupBy(row => (row._1, row._4)).toList.length)
    }

    test("write_to_hive_table_with_partition_and_then_distinct") {
        HiveSink.write(
            partitionedTable,
            partitionColumn = Some("p_dt"),
            timestampColumn = Some("modified_dt"),
            primaryColumns = Some(Seq("user_id", "p_dt")))(sampleDF)
        assert(session.sql(s"select * from $partitionedTable").count() == data.groupBy(row => (row._1, row._4)).toList.length)
    }
}
