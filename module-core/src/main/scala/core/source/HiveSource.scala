package core.source

import org.apache.spark.sql.SparkSession
import core.util.SparkUtil.StringExt

object HiveSource {

    def readStream(session: SparkSession, dstTable: String) = {
        ParquetSource.readStream(
            session,
            dstTable.toHivePath(),
            session.table(dstTable).schema
        )
    }
}
