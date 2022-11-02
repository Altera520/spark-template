package core.common

import core.util.SparkUtil.buildSparkSession
import core.util.TimeUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.Using

trait SparkBase {
    val logger = LoggerFactory.getLogger(this.getClass)
    var session: SparkSession = null

    def driver(session: SparkSession, args: Array[String]): Unit

    def main(args: Array[String]): Unit = {
        Using(buildSparkSession()) { session =>
            TimeUtil.watchTime {
                try {
                    driver(session, args)
                } catch {
                    case t: Throwable =>
                        logger.error("Application failed due to", t)
                }
            }
        }
    }
}
