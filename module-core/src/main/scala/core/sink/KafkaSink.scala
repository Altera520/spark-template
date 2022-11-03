package core.sink

import org.apache.spark.sql.DataFrame

object KafkaSink {
    def write(df: DataFrame, bootstrapServers : String, topic: String) = {
        df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", topic)
          .save()
    }

    def writeStream(df: DataFrame, bootstrapServers: String, topic: String) = {
        df.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", topic)
          .start()
    }
}
