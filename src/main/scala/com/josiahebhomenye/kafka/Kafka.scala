package com.josiahebhomenye.kafka

import java.util.Properties
import java.util.ResourceBundle.Control

import akka.pattern._
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import com.typesafe.config.Config
import net.ceedubs.ficus.readers.ValueReader
import org.apache.kafka.clients.consumer.{Consumer => KConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try


class KafkaCoordinator(config: Config) extends Actor{

  import KafkaCoordinator._
  import context._

  var consumers: Seq[ActorRef] = Seq.empty
  var shutdownRequestor = Option.empty[ActorRef]

  override def receive: Receive = {
    case CreateConsumer(topics, size, handler, factory) =>
      val ref = watch(actorOf(Consumers.groupProps(topics, size, handler, factory, config)))
      consumers = consumers :+ ref
      sender ! ref
    case Shutdown =>
      consumers.foreach(_ ! Consumers.Shutdown)
      shutdownRequestor = Some(sender)
    case Terminated(_) if consumers.size == 1 =>
      shutdownRequestor.foreach(_ ! Acknowledged)
      self ! PoisonPill
    case Terminated(ref) => consumers == consumers.filter(_ != ref)
  }
}

object KafkaCoordinator{

  case class CreateConsumer(topics: Seq[String], parallelism: Int, handler: EventHandler[_], factory: Properties => KConsumer[String, AnyRef])
  case object Shutdown
  case object ShutDownComplete
  case object ShutDownPending
  case object Yes
  case object No
  case object Acknowledged
}

object Kafka {

  var coordinator = Option.empty[ActorRef]
  val offline = new IllegalStateException("Kafka coordinator not online")

  def start(config: Config)(implicit sys: ActorSystem): Future[Unit] = coordinator match {
    case Some(_) => Future.successful()
    case None => coordinator = Some(sys.actorOf(Props(classOf[KafkaCoordinator], config)))
      Future.successful()
  }

  def stop(): Future[Unit] = coordinator match {
    case Some(c) => (c ? KafkaCoordinator.Shutdown).map(_ => ())
    case None => Future.failed(offline)
  }

  def createConsumer[T](topics: Seq[String], parallelism: Int, handler: EventHandler[_]
                        , factory: Properties => KConsumer[String, T]): Future[Control] = coordinator match {
    case Some(c) =>
      (c ? createConsumer(topics, parallelism, handler, factory)).mapTo[ActorRef].map(new Control(_))
    case None => Future.failed(offline)
  }

  implicit val propsReader: ValueReader[Properties] = new ValueReader[Properties] {
    override def read(config: Config, path: String): Properties = {
      Try(config.getConfig(path).entrySet().asScala.foldLeft(new Properties()) { (prop, e) =>
        prop.put(e.getKey, config.getString(s"$path.${e.getKey}")); prop
      }).toOption.getOrElse(new Properties())
    }
  }

}
