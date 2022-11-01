package core.sink

import core.util.JdbcUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

object JdbcSink {
    def write(dstTable: String,
              driver: String,
              url: String,
              user: String,
              password: String,
              saveMode: SaveMode)
             (df: DataFrame) = {

        df
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
}
