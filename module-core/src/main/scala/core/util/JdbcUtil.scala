package core.util

import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.lang3.StringUtils

import java.sql.ResultSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Using


object JdbcUtil {
    private val dbcpPool = mutable.HashMap[String, DBCP]()  // key: rdb url

    def add(dbcp: DBCP) = {
        dbcpPool.put(dbcp.url, dbcp)
    }

    def buildSql(sql: String, replacements: (String, Any)*) = {
        replacements.foldLeft(sql){ case (sql, (k, v)) =>
            sql.replaceAll("[$]\\{" + k + "\\}", v.toString)
        }
    }

    def selectThroughFile[T](url: String, path: String, replacements: (String, Any)*)(bind: ResultSet => T): List[T] = {
        val sql = Source.fromResource(path).mkString(StringUtils.EMPTY)
        execute(url, buildSql(sql, replacements: _*), bind)
    }

    def execute(url: String, sql: String): List[Null] = {
        def dummy(rs: ResultSet): Null = null
        execute(url, sql, dummy)
    }

    def execute[T](url: String, sql: String, bind: ResultSet => T): List[T] = {
        if (!dbcpPool.contains(url)) throw new NoSuchElementException
        val beans = ListBuffer[T]()
        Using.Manager { use =>
            val conn = use(dbcpPool(url).getConnection)
            val pstmt = use(conn.prepareStatement(sql))
            val rs = use(pstmt.executeQuery())
            while (rs.next()) beans += bind(rs)
        }
        beans.toList
    }

    def describe(url: String, dstTable: String): List[String] = {
        val sql = dbcpPool(url).driver match {
            // mysql
            case "com.mysql.cj.jdbc.Driver" => s"show columns from $dstTable"
            // oracle
            case "oracle.jdbc.driver.OracleDriver" => s"desc $dstTable"
            case _ => throw new IllegalArgumentException
        }

        execute(url, sql, rs => {
            rs.getString(0)
        })
    }
}

class DBCP(val driver: String,
           val url: String,
           user: String,
           password: String,
           maxTotal: Int = 8,
           maxIdle: Int = 8,
           minIdle: Int = 0,
           maxWaitMillis: Long = -1L) {
    private val ds = new BasicDataSource

    ds.setDriverClassName(driver)
    ds.setUrl(url)
    ds.setUsername(user)
    ds.setPassword(password)

    // maxTotal = maxIdle, maxTotal >= initialSize
    ds.setMaxTotal(maxTotal)

    // maxIdle >= minIdle, maxTotal = maxIdle
    ds.setMaxIdle(maxIdle)
    ds.setMinIdle(minIdle)

    ds.setMaxWaitMillis(maxWaitMillis)

    def getConnection = ds.getConnection
}
