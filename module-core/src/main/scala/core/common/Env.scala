package core.common

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader, ConfigSource}

import scala.reflect.ClassTag

object Environment extends Enumeration {
    val local = Value
    val dev = Value
    val prd = Value
}

object Env {
    lazy val mode = {
        val env = System.getProperty("APP_MODE", Environment.local.toString)
        Environment.withName(env.toLowerCase()).toString
    }

    def isLocalMode = {
        mode == Environment.local.toString
    }

    def buildConfigHint[T](): ProductHint[T] = {
        ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    }

    def getConfigOrThrow[T: ClassTag: ConfigReader]()(implicit productHint: ProductHint[T]): T = {
        // application.conf -> embedded resources/reference.conf 순으로 merge된다
        ConfigSource.default.at(mode).loadOrThrow[T]
    }

    def getConfigOrThrow[T: ClassTag: ConfigReader](app: String)(implicit productHint: ProductHint[T]): T = {
        ConfigSource.default.at(mode).at(app).loadOrThrow[T]
    }
}