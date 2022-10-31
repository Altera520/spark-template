package core.sink

import core.util.SqlUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

object JdbcSink {
    def write(dstTable: String,
              driver: String,
              url: String,
              user: String,
              password: String,
              saveMode: SaveMode)
             (df: DataFrame) = {

        //val columns = schema(dstTable, driver)

        //df.selectExpr(columns: _*) // sort column order
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

//    def schema(dstTable: String, driver: String) = {
//        driver match {
//            case "" => SqlUtil.select(s"show columns from $dstTable", _.getString(0))
//            case _ => new IllegalArgumentException()
//        }
//    }
}
