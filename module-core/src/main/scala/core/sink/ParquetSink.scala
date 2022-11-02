package core.sink

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode}

object ParquetSink {

    def write(df: DataFrame,
              saveLocation: String,
              saveMode: SaveMode) = {
        df.write
          .mode(saveMode)
          .option("parquet.enable.dictionary", "true")
          // 단위는 MB
          .option("parquet.block.size", s"${32 * 1024 * 1024}")
          .option("parquet.page.size", s"${2 * 1024 * 1024}")
          .option("parquet.dictionary.page.size", s"${8 * 1024 * 1024}")
          .parquet(saveLocation)
    }

    // TODO: MSCK REPAIR TABLE <table_name>해야 적재된 데이터를 hive에서 read 가능, 명령 수행없이 writeStream할수 있도록 수정필요
    def writeStream(df: DataFrame,
                    outputMode: OutputMode,
                    checkpointLocation: String,
                    path: String,
                    trigger: Trigger,
                    partitionColumn: Option[String] = None
                   ) = {
        val writer = df
          .writeStream

        (partitionColumn match {
            case Some(column) => writer.partitionBy(column)
            case _ => writer
        })
          .outputMode(outputMode)
          .format("parquet")
          .option("checkpointLocation", checkpointLocation)
          .option("path", path)
          .trigger(trigger)
          .start()
    }
}
