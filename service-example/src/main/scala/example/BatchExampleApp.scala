package example

import core.common.SparkBase
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat

object BatchExampleApp extends SparkBase {

    def read(tableName: String, partitionDate: String)(session: SparkSession): DataFrame = {
        session.sql(
            s"""
               |SELECT col1
               |     , col2
               |  FROM $tableName
               | WHERE p_dt = '$partitionDate'
               |""".stripMargin)
    }

    def process(df: DataFrame) = {
        import df.sparkSession.implicits._
        val sdf = new SimpleDateFormat("yyyyMMdd")

        df.select(
            $"col1" + lit(1) as "col1",
            $"col2" + lit(1) as "col2",
            lit(sdf.format(java.time.Instant.now().toEpochMilli)) as "p_dt"
        )
    }

    def write(tableName: String)(df: DataFrame) = {
        df.write
          .mode(SaveMode.Overwrite)
          .insertInto(tableName)
    }

    override def driver(session: SparkSession, args: Array[String]): Unit = {
        if (args.length < 1)
            throw new IllegalArgumentException

        val tableName = "dev.parquet_test"
        val partitionDate = args(0)
        val pipe = read(tableName, partitionDate) _ andThen process andThen write(tableName)
        pipe(session)
    }
}
