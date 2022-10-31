package core.util

import core.common.Env
import org.apache.commons.dbcp2.BasicDataSource
import pureconfig.generic.auto._

import java.sql.ResultSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Using

//object DBCP {
//    private val ds = new BasicDataSource
//    implicit val hint = Env.buildConfigHint[ReferenceConf]()
//    val conf = Env.getConfigOrThrow[ReferenceConf]()
//
//    ds.setDriverClassName(conf.dbDriverClassName)
//    ds.setUrl(conf.dbUrl)
//    ds.setUsername(conf.dbUsername)
//    ds.setPassword(conf.dbPassword)
//
//    // maxTotal = maxIdle, maxTotal >= initialSize
//    ds.setMaxTotal(conf.dsMaxTotal)
//
//    // maxIdle >= minIdle, maxTotal = maxIdle
//    ds.setMaxIdle(conf.dsMaxIdle)
//    ds.setMinIdle(conf.dsMinIdle)
//
//    ds.setMaxWaitMillis(conf.dsMaxWaitMillis)
//
//    def getConnection = ds.getConnection
//}

object SqlUtil {
    private val dbcpPool = mutable.HashMap[String, DBCP]()  // key: rdb url

    def buildSql(sql: String, replacements: (String, Any)*) = {
        replacements.foldLeft(sql){ case (sql, (k, v)) =>
            sql.replaceAll("[$]\\{" + k + "\\}", v.toString)
        }
    }

    def selectThroughFile[T](url: String, path: String, replacements: (String, Any)*)(bind: ResultSet => T): List[T] = {
        val sql = Source.fromResource(path).mkString("\n")
        select(url, buildSql(sql, replacements: _*), bind)
    }

    def select[T](url: String, sql: String, bind: ResultSet => T): List[T] = {
        val beans = ListBuffer[T]()
        Using.Manager { use =>
            val conn = use(dbcpPool(url).getConnection)
            val pstmt = use(conn.prepareStatement(sql))
            val rs = use(pstmt.executeQuery())
            while (rs.next()) beans += bind(rs)
        }
        beans.toList
    }

    def describe(dstTable: String) = {

    }

    def tmp() = {

    }
}

class DBCP(driver: String,
           url: String,
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
