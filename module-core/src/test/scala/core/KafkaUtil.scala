package core


import core.util.{KafkaAdminClientWrapper, TimeUtil}
import org.scalatest.funsuite.AnyFunSuite

class KafkaUtil extends AnyFunSuite {
    test("get_topic_partition_count") {
        val client = new KafkaAdminClientWrapper("bdp-dn1:9092,bdp-dn2:9092,bdp-dn3:9092")
        println(client.getPartitionCount("test-topic"))
    }

    test("get_offset_by_timestamp") {
        val client = new KafkaAdminClientWrapper("bdp-dn1:9092,bdp-dn2:9092,bdp-dn3:9092")
        println(client.getOffsetByTimestamp("test-topic", TimeUtil.dateToTimestamp("20221021")))
    }
}
