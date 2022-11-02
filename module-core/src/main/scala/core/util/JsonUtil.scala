package core.util
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

object JsonUtil {
    val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.registerModule(DefaultScalaModule)
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    def toJson(value: Map[Symbol, Any]): String = toJson(value map { case (k, v) => k.name -> v})
    def toJson(value: Any): String = mapper.writeValueAsString(value)
}
