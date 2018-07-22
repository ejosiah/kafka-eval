package com.josiahebhomenye.kafka

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, Terminated}
import akka.pattern._
import akka.routing._
import com.josiahebhomenye.kafka.KafkaCoordinator._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object Consumers{

  case object Subscribe
  case object Shutdown
  case object PollNext
  case object Commit
  case class CommitSuccess(offsets: Map[TopicPartition, OffsetAndMetadata])
  case class CommitFailed(ex: Throwable)
  case class HandlerFailure(ex: Throwable)

  def props(topics: Seq[String], handler: EventHandler[_], factory: Properties => Consumer[String, AnyRef], config: Config): Props = {
    Props(classOf[ConsumerActor], topics, handler, factory, config).withDispatcher("prio-dispatcher")
  }

  def groupProps(topics: Seq[String], parallelism: Int, handler: EventHandler[_], factory: Properties => Consumer[String, AnyRef], config: Config): Props = {
    Props(classOf[ConsumerGroup], topics, parallelism, handler, factory, config)
  }

}

class ConsumerGroup(topics: Seq[String],parallelism: Int, handler: EventHandler[_], factory: Properties => Consumer[String, AnyRef], config: Config) extends Actor with ActorLogging{
  import Consumers._
  import context._

  private var shutdownRequestor = Option.empty[ActorRef]
  private var activeConsumers = parallelism

  val router: Router = {
    Router(RoundRobinRoutingLogic(), Vector.fill(parallelism)(ActorRefRoutee(watch(actorOf(Consumers.props(topics, handler, factory, config))))))
  }

  override def receive: Receive = {
    case Subscribe => router.route(Broadcast(Subscribe), sender)
    case Shutdown =>
      shutdownRequestor = Some(sender)
      router.route(Broadcast(Shutdown), sender)
    case  Terminated(_) if activeConsumers == 1 =>
      shutdownRequestor.get ! ShutDownComplete
      context.stop(self)
    case Terminated(_) => activeConsumers = activeConsumers - 1
  }
}

class ConsumerActor(topics: Seq[String], handler: EventHandler[_], factory: Properties => Consumer[String, AnyRef], config: Config) extends Actor with ActorLogging{

  import context._
  import Consumers._

  private val props: Properties = {
    val generalProps  = config.as[Properties](s"app.kafka.consumer")
    val consumerProps = config.as[Properties](s"app.kafka.consumer.${handler.group}")
    consumerProps.asScala.foreach{case (k, v) => generalProps.put(k, v) }
    generalProps
  }

  private val autoCommit =  props.getProperty("enable.auto.commit").toBoolean
  private val timeout = (1 second).length
  private lazy val consumer = factory(props)

  override def receive: Receive = {
    case Subscribe =>
      consumer.subscribe(topics.asJava)
      sender ! Acknowledged
      self ! PollNext
    case PollNext =>
      log.info("poll request received")
      val records: Seq[ConsumerRecord[String, AnyRef]] = consumer.poll(timeout).asScala.toSeq
      handler.onTypeLess(records.map(handler.convert)).collect{
        case _ if records.nonEmpty && !autoCommit => Commit
        case _  => PollNext
      }.recover{case ex => HandlerFailure(ex)}  pipeTo self
    case Commit =>
      val receiver = self
      consumer.commitAsync((offsets, ex) => {
        if(ex != null){
          receiver ! CommitFailed(ex)
        }else{
          receiver ! CommitSuccess(offsets.asScala.toMap)
        }
      })
    case CommitSuccess(offsets) => handler.onCommit(offsets).map(_ => PollNext).recover{case ex => HandlerFailure(ex) } pipeTo self
    case CommitFailed(ex) => throw ex
    case HandlerFailure(ex) => throw ex
    case Shutdown =>
      log.info("shutdown request received")
      consumer.unsubscribe()
      consumer.close()
      self ! PoisonPill
      sender ! ShutDownComplete
  }
}

class Control(consumer: ActorRef){
  import Consumers._

  def start(): Future[Unit] = {
    (consumer ? Subscribe).map(_ => ())
  }

  def stop(): Future[Unit] = {
    (consumer ? Shutdown).map(_ => ())
  }
}

