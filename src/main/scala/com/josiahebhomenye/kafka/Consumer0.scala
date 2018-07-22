package com.josiahebhomenye.kafka

import java.util
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

import scala.collection.JavaConverters._

object Consumer0 extends ShutdownableThread("test_consumer", true) {
  val config: Config = ConfigFactory.load()
  val topic: String = config.getString("app.kafka.topic")


  val consumer: KafkaConsumer[Int, String] = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("app.kafka.server_url") + ":" + config.getInt("app.kafka.port"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString("app.kafka.client_id"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    new KafkaConsumer[Int, String](props)
  }

  consumer.subscribe(Seq(topic).asJava, new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = ()

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      consumer.seekToBeginning(partitions)
    }
  })

  def doWork(): Unit = {

    consumer.poll(1000).asScala.foreach{record =>
      println(s"received record with key: ${record.key()} with value: ${record.value()} at offset: ${record.offset()} of partition: ${record.partition()}")
    }
 //   shutdown()
  }

  def main(args: Array[String]): Unit = {
    println("starting consumer")
    Consumer0.start()
    Consumer0.awaitShutdown()
  }
}
