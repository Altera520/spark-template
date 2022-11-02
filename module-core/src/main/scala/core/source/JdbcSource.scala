package core.source

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcSource {
    def read(session: SparkSession,
             url: String,
             srcTable: String,
             user: String,
             password: String): DataFrame = {
        session.read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", srcTable)
          .option("user", user)
          .option("password", password)
          .load()
    }

    def readThroughSql(session: SparkSession,
                       url: String,
                       sql: String,
                       user: String,
                       password: String): DataFrame = {
        session.read
          .format("jdbc")
          .option("url", url)
          .option("sql", sql)
          .option("user", user)
          .option("password", password)
          .load()
    }
}
