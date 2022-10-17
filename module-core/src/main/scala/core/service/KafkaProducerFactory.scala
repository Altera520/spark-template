package core.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.collection.mutable

object KafkaProducerFactory {
    private val producers = mutable.HashMap[(String, String), KafkaProducerImpl]()

    def getProducer(bootstrapServers: String, topic: String) = {
        val key = (bootstrapServers, topic)
        if (!producers.contains(key)) {
            producers.put(key, new KafkaProducerImpl(bootstrapServers, topic))
        }
        producers(key)
    }
}

class KafkaProducerImpl(bootstrapServers: String, topic: String) {
    private val producer = new KafkaProducer[String, String]((() => {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props
    }).apply())

    private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    def produce(key: String, value: Object) = {
        producer.send(new ProducerRecord[String, String](this.topic, key, mapper.writeValueAsString(value)))
    }
}