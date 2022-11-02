package util

import com.dimafeng.testcontainers.{ForAllTestContainer, MySQLContainer}
import core.util.{DBCP, JdbcUtil}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.utility.DockerImageName

import scala.io.Source

class JdbcUtilSuite extends AnyFunSuite with ForAllTestContainer with BeforeAndAfterAll {

    private var dstTable: String = _

    override def container: MySQLContainer = MySQLContainer(
        mysqlImageVersion = DockerImageName.parse("mysql:8"),
        databaseName = "test-mysql",
        username = "test",
        password = "test")

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
            container.jdbcUrl,
            "test",
            password = "test"
        ))

        JdbcUtil.execute(container.jdbcUrl,
            s"""
               |create table if not exists $dstTable(
               | url varchar(100),
               | p_dt char(8)
               |)""".stripMargin)

        JdbcUtil.execute(container.jdbcUrl,
            s"""
               |insert into $dstTable values('www.naver.com', '20221031')
               |""".stripMargin)

        val desc = JdbcUtil.describe(container.jdbcUrl, dstTable)
    }
}
