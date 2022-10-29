package example

import core.common.{Env, SparkBase}
import core.source.KafkaSource
import example.entity.Conf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._
import sun.invoke.util.ValueConversions.cast

object StreamExampleApp extends SparkBase {
    implicit val hint = Env.buildConfigHint[Conf]()
    val conf = Env.getConfigOrThrow[Conf]()

    def readStream(topic: String)(session: SparkSession) = {
        val df = KafkaSource.readStream(session, conf.kafkaUrl, topic, 1000)
        import session.implicits._

        val schema = StructType(Seq())

        df.select(
            $"key".cast("string"),
            from_json($"value".cast("string"), schema) as "data"
        ).select($"data.*")
    }

    def process(df: DataFrame) = {
        //Window.partitionBy().orderBy()
        df
    }

    def writeStream(df: DataFrame) = {

    }

    override def driver(session: SparkSession, args: Array[String]): Unit = {
        val topic = args(0)
        val pipe = readStream(topic) _ andThen process andThen writeStream
        pipe(session)
    }
}
