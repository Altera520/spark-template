package core.sink

import org.apache.spark.sql.{DataFrame, SaveMode}

object ParquetSink {

    def write(df: DataFrame,
              saveLocation: String,
              saveMode: SaveMode) = {
        df.write
          .mode(saveMode)
          .option("parquet.enable.dictionary", "true")
          .option("parquet.block.size", s"${32 * 1024 * 1024}")
          .option("parquet.page.size", s"${2 * 1024 * 1024}")
          .option("parquet.dictionary.page.size", s"${8 * 1024 * 1024}")
          .parquet(saveLocation)
    }
}
