package core

import core.util.{JsonUtil, JsonWrapper, TimeUtil}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalDateTime, ZoneId}

class TimeUtil extends AnyFunSuite {

    test("config_mapping_test") {
        def mkJson(partition: Int, timestamp: Long) = {
            val partitionWithTs = 0 until partition map { index =>
                s""""$index": $timestamp"""
            } mkString ","
            s"""{"hello": {$partitionWithTs}}"""
        }

        print(mkJson(3, TimeUtil.dateToTimestamp("20221011")))
    }

    test("date_to_epoch_milli_test") {
        // given
        val epoch = TimeUtil.dateToTimestamp("20221024")

        // when
        val dt = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(epoch), ZoneId.of("UTC"))

        // then
        assert(LocalDate.of(2022, 10, 24) == dt.toLocalDate)
    }

    test("tt") {
        case class Te(url: String, timestamp: Long)
//        import io.circe._
//        import io.circe.generic.auto._
//        import io.circe.parser._
//        import io.circe.syntax._

        //println(Te("www.namver.com", java.time.Instant.now().toEpochMilli).asJson.noSpaces)

        print(JsonUtil.toJson(Map("url" -> "www.naver.com")))

//        val out = JsonUtil.toJson(Map(
//            "url" -> "www",
//            "timestamp" -> 15203030
//        ))
//        println(out)

    }
}