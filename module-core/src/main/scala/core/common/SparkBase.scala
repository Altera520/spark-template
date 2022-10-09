package core.common

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

trait SparkBase {
    val logger = LogManager.getRootLogger
    var session: SparkSession = null

    def buildSparkSession(): SparkSession = {
        var sessionBuilder = SparkSession.builder().enableHiveSupport()
        if (Env.isLocalMode)
            sessionBuilder = sessionBuilder.master("local[*]")
        sessionBuilder.getOrCreate()
    }

    def driver(session: SparkSession, args: Array[String]): Unit

    def main(args: Array[String]): Unit = {
        session = buildSparkSession()
        try {
            driver(session, args)
        } catch {
            case t: Throwable =>
                logger.error("Application failed due to", t)
                session.stop()
        }
    }
}
