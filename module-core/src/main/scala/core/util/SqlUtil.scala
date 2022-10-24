package core.util

import core.common.Env
import core.entity.ReferenceConf
import org.apache.commons.dbcp2.BasicDataSource
import pureconfig.generic.auto._

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Using

object DBCP {
    private val ds = new BasicDataSource
    implicit val hint = Env.buildConfigHint[ReferenceConf]()
    val conf = Env.getConfigOrThrow[ReferenceConf]()

    ds.setDriverClassName(conf.dbDriverClassName)
    ds.setUrl(conf.dbUrl)
    ds.setUsername(conf.dbUsername)
    ds.setPassword(conf.dbPassword)

    // maxTotal = maxIdle, maxTotal >= initialSize
    ds.setMaxTotal(conf.dsMaxTotal)

    // maxIdle >= minIdle, maxTotal = maxIdle
    ds.setMaxIdle(conf.dsMaxIdle)
    ds.setMinIdle(conf.dsMinIdle)

    ds.setMaxWaitMillis(conf.dsMaxWaitMillis)

    def getConnection = ds.getConnection
}

object SqlUtil {
    def selectThroughFile[T](path: String, replacements: (String, Any)*)(bind: ResultSet => T): List[T] = {
        var sql = Source.fromResource(path).mkString("\n")
        sql = replacements.foldLeft(sql)((sql, replacements) => {
            val (k, v) = replacements
            sql.replaceAll("[$]\\{" + k + "\\}", v.toString)
        })
        select(sql, bind)
    }

    def select[T](sql: String, bind: ResultSet => T): List[T] = {
        val beans = ListBuffer[T]()
        Using.Manager { use =>
            val conn = use(DBCP.getConnection)
            val pstmt = use(conn.prepareStatement(sql))
            val rs = use(pstmt.executeQuery())
            while (rs.next()) beans += bind(rs)
        }
        beans.toList
    }
}
