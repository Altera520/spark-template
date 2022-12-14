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
     *
     * @return
     */
    def buildSparkSession(): SparkSession = {
        val sessionBuilder = SparkSession
          .builder()
          .enableHiveSupport()
          // allow hive dynamic partition
          .config("hive.exec.dynamic.partition", "true") // 동적 partition 수행 허용
          .config("hive.exec.dynamic.partition.mode", "nonstrict") // partition key 자동생성

        (if (Env.isLocalMode)
            sessionBuilder
              .master("local[*]")
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
     *
     * @param session
     * @param postProcess
     */
    def measurementTPS(session: SparkSession, postProcess: TpsMetric => Unit) = {
        val service = new TpsMeasurementService
        service(session, postProcess)
    }

    /**
     * executor와 core수에 맞게 리파티션 진행
     *
     * @param session
     * @param df
     * @return
     */
    def optimizeRepartition(session: SparkSession, df: DataFrame) = {
        val numExecutor = session.conf.get("spark.executor.instances", "1").toInt
        val numExecutorCore = session.conf.get("spark.executor.cores", "1").toInt
        val numPartition = numExecutor * numExecutorCore

        // repartition
        if (numPartition <= 1) df
        else df.repartition(numPartition)
    }

    /**
     * 문자열 확장 클래스
     *
     * @param str
     */
    implicit class StringExt(str: String) {
        /**
         * 테이블명으로부터 hdfs상의 hive location 반환
         *
         * @return
         */
        def toHivePath() = {
            val prefix = getHiveWarehousePath
            val Array(schemaName, tableName) =
                if (str.indexOf(".") > 0) str.split("\\.")
                else Array("default", str)

            Env.isLocalMode match {
                case true => s"$prefix/$tableName"
                case _ => s"$prefix/$schemaName.db/$tableName"
            }
        }
    }

    /**
     * 환경(local, dev, prd)에 맞는 hive warehouse path 반환
     *
     * @return
     */
    def getHiveWarehousePath() = {
        Env.isLocalMode match {
            case true => s"file://${sys.props("user.dir")}/spark-warehouse"
            case _ => "/user/hive/warehouse"
        }
    }

    def mkCheckpointLocation(postfix: String,
                             prefix: String = s"/user/${sys.env.get("USER").getOrElse("default")}/spark-checkpoints") = {
        s"$prefix/$postfix"
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