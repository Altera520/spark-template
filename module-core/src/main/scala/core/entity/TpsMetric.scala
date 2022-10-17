package core.entity

case class TpsMetric (totalSec: Long,
                      min: Long,
                      sec: Long,
                      start: String,
                      end: String,
                      inputRows: Long,
                      outputRows: Long,
                      tps: Long)
