package core.util

import core.common.Env
import org.apache.spark.sql.SparkSession

object SparkUtil {
    /**
     * local, dev, prd 환경에 맞는 SparkSession 생성
     * @return
     */
    def buildSparkSession(): SparkSession = {
        var sessionBuilder = SparkSession.builder().enableHiveSupport()
        if (Env.isLocalMode)
            sessionBuilder = sessionBuilder.master("local[*]")
        sessionBuilder.getOrCreate()
    }

    /**
     * 문자열 확장 클래스
     * @param str
     */
    implicit class StringExt(str: String) {
        /**
         * 테이블명으로부터 hdfs상의 hive location 반환
         * @return
         */
        def toHivePath() = {
            val Array(schemaName, tableName) =
                if(str.indexOf(".") > 0) str.split("\\.")
                else Array("default", str)
            s"/user/hive/warehouse/$schemaName.db/$tableName"
        }
    }
}
