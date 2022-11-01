package core.sink

import core.util.JdbcUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

object JdbcSink {

    /**
     * dataframe을 jdbc에 write
     *
     * @param dstTable
     * @param driver
     * @param url
     * @param user
     * @param password
     * @param saveMode
     * @param df
     */
    def write(dstTable: String,
              driver: String,
              url: String,
              user: String,
              password: String,
              saveMode: SaveMode)
             (df: DataFrame) = {
        import df.sparkSession.implicits._

        df.select(JdbcUtil.describe(url, dstTable).map(c => $"$c"): _*)   // 스키마에 맞게 재정렬
          .write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", dstTable)
          .mode(saveMode)
          .save()
    }

    def writeThroughSql(dstTable: String,
                        driver: String,
                        url: String,
                        user: String,
                        password: String,
                        saveMode: SaveMode)
                       (sql: String)= {

    }
}
