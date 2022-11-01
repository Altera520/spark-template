package core.sink

import core.util.SparkUtil
import core.util.SparkUtil.StringExt
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HiveSink {

    /**
     * hive 테이블에 write 수행, timestampColumn과 primaryColumn이 None이 아니면 distinct가 수행된다.
     *
     * @param dstTable 적재대상 테이블
     * @param partitionColumn partition으로 나누어진 테이블에는 지정해야한다.
     * @param timestampColumn CDC(timestamp on rows) 대상 컬럼
     * @param primaryColumns pk
     * @param df
     * @return
     */
    def write(dstTable: String,
              partitionColumn: Option[String] = None,
              timestampColumn: Option[String] = None,
              primaryColumns: Option[Seq[String]] = None)(df: DataFrame): Unit = {
        val session = df.sparkSession

        if (session.catalog.tableExists(dstTable)) insertInto(dstTable, SaveMode.Append)(df)
        else saveAsTable(dstTable, partitionColumn)(df)

        (timestampColumn, primaryColumns, df.rdd.isEmpty) match {
            case (Some(timestampCol), Some(primaryCols), false) =>
                val distinctedDF = distinctTable(dstTable, partitionColumn, timestampCol, primaryCols)(df)
                insertInto(dstTable, SaveMode.Overwrite)(distinctedDF)
            case _ =>
        }
    }

    /**
     * hive 테이블에 데이터 overwrite or append
     *
     * @param dstTable
     * @param saveMode
     * @param df
     * @return
     */
    def insertInto(dstTable: String, saveMode: SaveMode)(df: DataFrame) = {
        val session = df.sparkSession
        val columns = schema(session, dstTable)

        df.selectExpr(columns: _*) // sort column order
          .write
          .mode(saveMode)
          .insertInto(dstTable)
        df
    }

    /**
     * drop table 수행후 create table
     *
     * @param dstTable
     * @param partitionColumn
     * @param df
     * @return
     */
    def saveAsTable(dstTable: String, partitionColumn: Option[String] = None)(df: DataFrame) = {
        var writer = df.write
          .format("parquet")

        writer = partitionColumn match {
            case Some(column) => writer.partitionBy(column)
            case _ => writer
        }
        writer.option("path", dstTable.toHivePath)
          .saveAsTable(dstTable)
    }

    /**
     * CDC(timestamp on rows) 방식을 사용하여 중복데이터중 오래된 row들은 제거
     *
     * @param primaryColumns
     */
    def distinctTable(dstTable: String,
                      partitionColumn: Option[String],
                      timestampColumn: String,
                      primaryColumns: Seq[String])(df: DataFrame) = {
        val session = df.sparkSession
        import session.implicits._

        val windowFunc = Window
          .partitionBy(primaryColumns.map(col): _*)
          .orderBy($"$timestampColumn".desc)

        val whereStmt = partitionColumn match {
            case Some(column) => {
                val values = getPartitionValues(column)(df)
                s"where $column in (${values.map(col => s"'${col}'").mkString(",")})"
            }
            case _ => StringUtils.EMPTY
        }
        session.sql(
            s"""
               |select *
               |  from $dstTable
               | $whereStmt
               |""".stripMargin)
          .withColumn("row_no", row_number().over(windowFunc))
          .where($"row_no" === lit(1))
          .drop($"row_no")
    }

    /**
     * hive 테이블의 column 정보 반환
     *
     * @param session
     * @param table
     * @return
     */
    def schema(session: SparkSession, table: String) = {
        import session.implicits._
        val columns = session.catalog.listColumns(table).map(_.name).collect().toList
        columns
    }

    /**
     * df에 포함된 partition 컬럼의 value들을 반환
     * @param partitionColumns
     * @param df
     * @return
     */
    def getPartitionValues(partitionColumns: String)(df: DataFrame) = {
        import df.sparkSession.implicits._
        df
          .map(_.getAs[String](partitionColumns) -> 1)
          .groupByKey(_._1)
          .count()
          .collect()
          .map(_._1)
    }

    def writeStream(df: DataFrame,
                    outputMode: OutputMode,
                    dstTable: String,
                    trigger: Trigger,
                    checkpointLocation: Option[String] = None,
                    partitionColumn: Option[String] = None) = {
        ParquetSink.writeStream(
            df,
            outputMode,
            checkpointLocation match {
                case Some(location) => location
                case _ => SparkUtil.mkCheckpointLocation(dstTable)
            },
            dstTable.toHivePath(),
            trigger,
            partitionColumn)
    }
}
