package com.josiahebhomenye.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.annotation.tailrec
import scala.language.implicitConversions

object Producer{
  val config: Config = ConfigFactory.load()
  val topic: String = config.getString("app.kafka.topic")

  implicit def convert(func: (RecordMetadata, Exception) => Unit): Callback = (metadata: RecordMetadata, exception: Exception) => func(metadata, exception)

  val producer: KafkaProducer[Int, String] = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("app.kafka.server_url") + ":" + config.getInt("app.kafka.port"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getString("app.kafka.client_id"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[Int, String](props)
  }

  def run(): Unit = {

    val doc = Jsoup.connect("https://livelifehappy.com/random-life-quotes/").get()
    val elements: Seq[Element] = doc.select("p").iterator().asScala.toSeq.toList

    @tailrec
    def send(messageNo: Int, elements: Seq[Element]): Unit = {
      if (elements.isEmpty) return
      if (elements.head.hasText) {
        val elm = elements.head
        val msg = elm.text()
        val startTime = System.currentTimeMillis()

        def callback(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            val elapsedTime = System.currentTimeMillis() - startTime
            println(s"$messageNo, $msg send to partition(${metadata.partition()}), offset(${metadata.offset()}) in $elapsedTime")
          }
        }

        producer.send(new ProducerRecord[Int, String](topic, messageNo, msg), callback)

      }
      send(messageNo + 1, elements.tail)
    }

    send(1, elements)
    println("producer done")
  }
}

object Runner extends App{

  Producer.run()

}
