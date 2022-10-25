package core.util

import core.common.Env
import core.entity.TpsMetric
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.atomic.AtomicLong

object SparkUtil {
    private val outputRows = new AtomicLong()

    /**
     * local, dev, prd 환경에 맞는 SparkSession 생성
     * @return
     */
    def buildSparkSession(): SparkSession = {
        val sessionBuilder = SparkSession.builder().enableHiveSupport()
        (if (Env.isLocalMode)
            sessionBuilder.master("local[*]")
        else {
            sessionBuilder
              // allow hive dynamic partition
              .config("hive.exec.dynamic.partition", "true")
              .config("hive.exec.dynamic.partition", "nonstrict")
              .config("hive.input.dir.recursive", "true")
              .config("hive.mapred.supports.subdirectories", "true")
              .config("hive.supports.subdirectories", "true")
              .config("mapred.input.dir.recursive", "true")
              .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
        }).getOrCreate()
    }

    /**
     * spark streams에서 TPS 측정
     * @param spark
     */
    def measurementTPS(spark: SparkSession, bootstrapServers: String, key: String) = {
        val context = spark.sparkContext
        val producer = KafkaUtil.getProducer(bootstrapServers, Env.referenceConf.tpsMetric)
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
     * executor와 core수에 맞게 리파티션 진행
     * @param session
     * @param df
     * @return
     */
    def appropriateRepartition(session: SparkSession, df: DataFrame) = {
        val numExecutor = session.conf.get("spark.executor.instances", "1").toInt
        val numExecutorCore = session.conf.get("spark.executor.cores", "1").toInt
        val numPartition = numExecutor * numExecutorCore

        // repartition
        if (numPartition <= 1) df
        else df.repartition(numPartition)
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
