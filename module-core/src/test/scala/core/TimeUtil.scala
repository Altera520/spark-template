package core

import core.util.TimeUtil
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
}