package util

import core.util.{DBCP, JdbcUtil}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class JdbcUtilSuite extends AnyFunSuite with BeforeAndAfterAll {

    private var dstTable: String = _

    override protected def beforeAll(): Unit = {
        super.beforeAll()

    }

    test("build_sql") {
        val sql = Source.fromResource("sql/select.access_log_with_where.sql").mkString("")
        val dt = "20221031"
        val buildSql = JdbcUtil.buildSql(sql, ("p_dt", dt)).split("\n").last.trim
        assert(buildSql == s"where p_dt = '$dt'")
    }

    test("get_schema") {
        dstTable = "access_log"
        JdbcUtil.add(new DBCP(
            "com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://localhost:3306/pipeline",
            "scott",
            "tiger"
        ))

        val desc = JdbcUtil.describe("jdbc:mysql://localhost:3306/pipeline", "property_stat")
        println()
    }
}
