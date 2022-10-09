package test

import core.common.Env
import core.model.Conf
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite {

    test("test") {
        val conf = Env.conf[Conf]
        println(conf)
    }
}