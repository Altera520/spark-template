package example

import core.common.{Env, SparkBase}
import core.source.KafkaSource
import example.entity.Conf
import org.apache.spark.sql.functions.{from_json, get_json_object}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._

object StreamExampleApp extends SparkBase {
    implicit val hint = Env.buildConfigHint[Conf]()
    val conf = Env.getConfigOrThrow[Conf]()

    def readStream(topic: String)(session: SparkSession) = {
        val df = KafkaSource.readStream(session, conf.kafkaUrl, topic, 1000)
        import session.implicits._

        df.select(get_json_object($"value".cast("string"), "$.payload") as "data")
    }

    def process(df: DataFrame) = {
        df
    }

    def writeStream(df: DataFrame) = {
        df.writeStream
    }

    override def driver(session: SparkSession, args: Array[String]): Unit = {
        val topic = args(0)
        val pipe = readStream(topic) _ andThen process andThen writeStream
        pipe(session)
    }
}
