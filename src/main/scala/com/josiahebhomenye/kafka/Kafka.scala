package com.josiahebhomenye.kafka

import java.util.Properties

import scala.collection.JavaConverters._
import akka.actor.{Actor, ActorSystem}
import com.typesafe.config.Config
import net.ceedubs.ficus.readers.ValueReader
import org.apache.kafka.clients.consumer.{Consumer => KConsumer}

import scala.concurrent.Future
import scala.util.Try


class KafkaCoordinator extends Actor{

  override def receive: Receive = ???
}

object KafkaCoordinator{

  case class CreateConsumer[T](topics: Seq[String], parallelism: Int, handler: EventHandler[_], factory: Unit => KConsumer[String, T])
  case object Shutdown
  case object ShutDownComplete
  case object ShutDownPending
  case object Yes
  case object No
  case object Acknowledged
}

object Kafka {

  def start(implicit sys: ActorSystem): Future[Unit] = ???

  def stop(): Future[Unit] = ???

  def consume[T](topics: Seq[String], parallelism: Int, handler: EventHandler[_], factory: Unit => KConsumer[String, T]): Control = ???

  implicit val propsReader: ValueReader[Properties] = new ValueReader[Properties] {
    override def read(config: Config, path: String): Properties = {
      Try(config.getConfig(path).entrySet().asScala.foldLeft(new Properties()) { (prop, e) =>
        prop.put(e.getKey, config.getString(s"$path.${e.getKey}")); prop
      }).toOption.getOrElse(new Properties())
    }
  }

}
