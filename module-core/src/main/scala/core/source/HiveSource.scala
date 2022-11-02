package core.source

import org.apache.spark.sql.SparkSession
import core.util.SparkUtil.StringExt

object HiveSource {

    /**
     * hive 테이블을 스트림 방식으로 read
     *
     * @param session
     * @param dstTable
     * @return
     */
    def readStream(session: SparkSession, dstTable: String) = {
        ParquetSource.readStream(
            session,
            dstTable.toHivePath(),
            session.table(dstTable).schema
        )
    }
}
