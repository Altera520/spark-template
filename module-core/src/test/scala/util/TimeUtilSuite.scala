package util

import core.util.TimeUtil
import org.scalatest.funsuite.AnyFunSuite

class TimeUtilSuite extends AnyFunSuite {
    test("") {
        val pattern = "yyyyMMdd"
        val regexPattern = "[0-9]{8}".r
        assert(regexPattern matches TimeUtil.epochMillisToDateString(java.time.Instant.now().toEpochMilli, pattern))
    }
}
