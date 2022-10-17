package core.util

import core.common.Env
import core.entity.TpsMetric
import core.service.KafkaProducerFactory
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.atomic.AtomicLong

object SparkUtil {
    val outputRows = new AtomicLong()

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
     * spark streams에서 TPS 측정
     * @param spark
     */
    def measurementTPS(spark: SparkSession, bootstrapServers: String, key: String) = {
        val context = spark.sparkContext
        val producer = KafkaProducerFactory.getProducer(bootstrapServers, "tps-metric")
        val utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        val kscFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        context.addSparkListener(new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
                outputRows.getAndAdd(taskEnd.taskMetrics.outputMetrics.recordsWritten)
            }
        })

        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

            override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
                val totalSec = Math.max(event.progress.batchDuration / 1000, 1)
                val tpsMetric = TpsMetric(
                    totalSec,
                    min = totalSec / 60,
                    sec = totalSec % 60,
                    start = kscFormat.format(utcFormat.parse(event.progress.timestamp).getTime),
                    end = kscFormat.format(java.time.Instant.now().toEpochMilli),
                    inputRows = event.progress.numInputRows,
                    outputRows = outputRows.get(),
                    tps = outputRows.get() / totalSec
                )

                producer.produce(key, tpsMetric)
                outputRows.set(0)
            }

            override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
        })
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
