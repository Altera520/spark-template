package core.common

import core.util.SparkUtil.buildSparkSession
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.Using

trait SparkBase {
    val logger = LogManager.getLogger(this.getClass.getName)
    var session: SparkSession = null

    def driver(session: SparkSession, args: Array[String]): Unit

    def main(args: Array[String]): Unit = {
        Using(buildSparkSession()) { session =>
            try {
                driver(session, args)
            } catch {
                case t: Throwable =>
                    logger.error("Application failed due to", t)
            }
        }
    }
}
