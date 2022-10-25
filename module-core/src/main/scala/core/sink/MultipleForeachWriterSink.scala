package core.sink

import org.apache.spark.sql.{ForeachWriter, Row}

import scala.collection.mutable.ArrayBuffer

class MultipleForeachWriterSink extends ForeachWriter[Row] {

    protected val writers = new ArrayBuffer[ForeachWriter[Row]]()

    def add(t: ForeachWriter[Row]): Unit = {
        writers.append(t)
    }

    override def open(partitionId: Long, epochId: Long): Boolean = {
        writers.foreach(_.open(partitionId, epochId))
        true
    }

    override def process(value: Row): Unit = {
        writers.foreach(_.process(value))
    }

    override def close(errorOrNull: Throwable): Unit = {
        writers.foreach(_.close(errorOrNull))
    }
}
