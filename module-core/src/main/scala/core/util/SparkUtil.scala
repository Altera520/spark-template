package core.util

import core.common.Env
import core.entity.TpsMetric
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.TimeZone

object SparkUtil {

    /**
     * local, dev, prd 환경에 맞는 SparkSession 생성
     * @return
     */
    def buildSparkSession(): SparkSession = {
        val sessionBuilder = SparkSession
          .builder()
          .enableHiveSupport()
          // allow hive dynamic partition
          .config("hive.exec.dynamic.partition", "true")            // 동적 partition 수행 허용
          .config("hive.exec.dynamic.partition.mode", "nonstrict")  // partition key 자동생성

        (if (Env.isLocalMode)
            sessionBuilder
              .master("local[*]")
              // use embedded hive (in-memory instance of Apache Derby as stand in for Hive database)
//              .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
//              .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:default;create=true")
//              .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
//              .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
        else {
            sessionBuilder
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
    def measurementTPS(session: SparkSession, postProcess: TpsMetric => Unit) = {
        val service = new TpsMeasurementService
        service(session, postProcess)
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


private[util] class TpsMeasurementService {
//    private var outputRows: Long = 0

    def apply(session: SparkSession, postProcess: TpsMetric => Unit) = {
//        val context = session.sparkContext
        val utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        val kscFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

//        context.addSparkListener(new SparkListener() {
//            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//                println("####### end task #########")
//                println(s"stageId: ${taskEnd.stageId}, taskId: ${taskEnd.taskInfo.taskId}, executorId: ${taskEnd.taskInfo.executorId}")
//                synchronized {
//                    // driver <- executor
//                    outputRows += taskEnd.taskMetrics.outputMetrics.recordsWritten
//                }
//            }
//        })

        session.streams.addListener(new StreamingQueryListener() {
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
                    outputRows = event.progress.sink.numOutputRows,
                    tps = event.progress.sink.numOutputRows / totalSec
                )

                postProcess(tpsMetric)
                //outputRows = 0
            }

            override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
        })
    }
}