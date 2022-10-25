package core.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ParquetSource {

    def readStream(session: SparkSession,
                   readLocation: String,
                   schema: StructType) = {
        session.readStream
          .format("parquet")
          .schema(schema)
          .load(readLocation)
    }

    def read() = {

    }
}
