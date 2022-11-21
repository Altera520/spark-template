package core.util

import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.lang3.StringUtils

import java.sql.ResultSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Using}


object JdbcUtil {
    private val dbcpPool = mutable.HashMap[String, DBCP]()  // key: rdb url

    /**
     * DBCP 추가
     * @param dbcp DBCP 인스턴스
     * @return
     */
    def add(dbcp: DBCP) = {
        dbcpPool.put(dbcp.url, dbcp)
    }

    /**
     * sql에서 ${key} 형식의 문자열을 치환
     * @param sql sql 문자열
     * @param replacements 치환할 값
     * @return
     */
    def buildSql(sql: String, replacements: (String, Any)*) = {
        replacements.foldLeft(sql){ case (sql, (k, v)) =>
            sql.replaceAll("[$]\\{" + k + "\\}", v.toString)
        }
    }

    /**
     * sql file을 통해 select 수행
     * @param url jdbc url
     * @param path resources에 위치한 sql 파일경로
     * @param replacements 치환할 값
     * @param bind ResultSet을 매핑 처리할 함수
     * @tparam T
     * @return
     */
    def selectThroughFile[T](url: String, path: String, replacements: (String, Any)*)(bind: ResultSet => T): List[T] = {
        val sql = Source.fromResource(path).mkString(StringUtils.EMPTY)
        select(url, buildSql(sql, replacements: _*), bind)
    }

    /**
     * bind를 수행하는 쿼리함수
     * @param url
     * @param sql
     * @param bind
     * @tparam T
     * @return
     */
    def select[T](url: String, sql: String, bind: ResultSet => T): List[T] = {
        if (!dbcpPool.contains(url)) throw new NoSuchElementException
        //val beans = ListBuffer[T]()
        Using.Manager { use =>
            val conn = use(dbcpPool(url).getConnection)
            val pstmt = use(conn.prepareStatement(sql))
            val rs = use(pstmt.executeQuery())

            new Iterator[T] {
                def hasNext = rs.next()
                def next(): T = bind(rs)
            }.toList
        } match {
            case Success(value) => value
            case Failure(exception) => throw exception
        }
        //beans.toList
    }

    /**
     * 테이블 스키마 추출
     * @param url jdbc url
     * @param dstTable 스키마추출 대상 테이블
     * @return
     */
    def describe(url: String, dstTable: String): List[String] = {
        val sql = dbcpPool(url).driver match {
            // mysql, mariadb
            case x if Seq(
                "org.mariadb.jdbc.Driver",
                "com.mysql.cj.jdbc.Driver").contains(x) => s"describe $dstTable"
            case _ => throw new IllegalArgumentException
        }

        select(url, sql, rs => {
            rs.getString("Field")
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
