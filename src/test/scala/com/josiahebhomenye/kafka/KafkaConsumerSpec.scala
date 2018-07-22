package com.josiahebhomenye.kafka

import java.util
import java.util.Properties

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, MockConsumer, OffsetAndMetadata, OffsetResetStrategy, Consumer => KConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterEach, Matchers}
import akka.pattern._
import akka.util.Timeout

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

class KafkaConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with AsyncWordSpecLike with Matchers with BeforeAndAfterEach{
  import KafkaConsumerSpec._
  val consumer = new TestConsumer
  val topic = consumer.topic
  implicit val timeout = Timeout(30 seconds)

  def this(){
    this(ActorSystem("Kafka", KafkaConsumerSpec.config))
  }



  "Kafka Consumer" should {
    "receive ConsumerRecords when its online" in {
      consumer.noOfExpectedRecords = 5
      val ref = system.actorOf(ConsumerActor.groupProps(Seq(topic), 1, consumer, consumer.consumerFactory, KafkaConsumerSpec.config))
      for{
        _ <- ref ? ConsumerActor.Subscribe
        _ =  for( i <- 0 until   5 ) consumer.addRecord(new ConsumerRecord[String, AnyRef](topic, 0, i, "", i.toString))
        consumed <- consumer.consumed().map(_.map(_.value))
        commitOffset <- consumer.commitedOffeset()
        _ <- ref ? KafkaCoordinator.Shutdown
      } yield{
        consumer.closed() shouldBe true
        commitOffset shouldBe 5
        consumed should contain allElementsOf Seq("0", "1", "2", "3", "4")
      }
    }

    "not call eventHandler handle onCommit method" in {
      consumer.noOfExpectedRecords = 5
      val ref = system.actorOf(ConsumerActor.groupProps(Seq(topic), 1, consumer, consumer.consumerFactory, configWithAutoCommit))
      for{
        _ <- ref ? ConsumerActor.Subscribe
        _ =  for( i <- 0 until   5 ) consumer.addRecord(new ConsumerRecord[String, AnyRef](topic, 0, i, "", i.toString))
        consumed <- consumer.consumed().map(_.map(_.value))
        _ <- ref ? KafkaCoordinator.Shutdown
      } yield{
        consumer.closed() shouldBe true
        consumer.commited.isCompleted shouldBe false
        consumed should contain allElementsOf Seq("0", "1", "2", "3", "4")
      }
    }
  }

  override protected def beforeEach(): Unit = {
    consumer.reset()
  }
}

object KafkaConsumerSpec{

  implicit def convert(testConsumer: TestConsumer): MockConsumer[String, AnyRef] = testConsumer.mockConsumer

  val config = ConfigFactory.parseString(
    """
      |akka{
      |   loggers = ["akka.testkit.TestEventListener"]
      |   loglevel = "WARNING"
      |}
      |
      |app.kafka.consumer: {
      |   enable.auto.commit: false
      |}
      |
      |prio-dispatcher {
      |  mailbox-type = "com.josiahebhomenye.kafka.ShutdownFirstMailBox"
      |}
      |
    """.stripMargin)

    val configWithAutoCommit = ConfigFactory.parseString(
      """
        |akka{
        |   loggers = ["akka.testkit.TestEventListener"]
        |   loglevel = "WARNING"
        |}
        |
        |app.kafka.consumer: {
        |   enable.auto.commit: true
        |}
        |
        |prio-dispatcher {
        |  mailbox-type = "com.josiahebhomenye.kafka.ShutdownFirstMailBox"
        |}
        |
    """.stripMargin)
}

class TestConsumer extends  EventHandler[String] {

  implicit val ec = ExecutionContext.global
  val partitions = Seq(new TopicPartition(topic, 0))
  var returnFromOnEvent = true
  var noOfExpectedRecords = 1

  var consumedEvents: Seq[Event[String]] = Seq.empty
  var promise = Promise[Unit]()
  var dataReady = Promise[Seq[Event[String]] ]()
  var commited = Promise[Long]()

  var mockConsumer: MockConsumer[String, AnyRef] = createMockConsumer

  def group: String = "Test_group"

  def topic: String = "test_topic"

  def deserializer: Class[_] = classOf[StringDeserializer]

  val consumerFactory: Properties => KConsumer[String, AnyRef] = _ => mockConsumer

  def on(events: Seq[Event[String]]): Future[Unit] = {
    consumedEvents = consumedEvents ++: events.toList
    if (events.nonEmpty && consumedEvents.size >= noOfExpectedRecords){
      dataReady.trySuccess(consumedEvents)
    }

    if (returnFromOnEvent) promise.trySuccess()

    promise.future
  }


  override def onCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Unit] = {
    commited.trySuccess(offsets.head._2.offset())
    commited.future.map(_ => Unit)
  }

  def consumed(): Future[Seq[Event[String]] ] = {
    dataReady.future
  }

  def commitedOffeset(): Future[Long] ={
    commited.future
  }

  def reset(): Unit = {
    consumedEvents = Seq.empty
    promise = Promise()
    dataReady = Promise()
    commited = Promise()
    mockConsumer = createMockConsumer
    returnFromOnEvent = true
  }

  private def createMockConsumer = {
    new MockConsumer[String, AnyRef](OffsetResetStrategy.EARLIEST) {
      override def subscribe(topics: util.Collection[String], listener: ConsumerRebalanceListener): Unit = {
        super.subscribe(topics, listener)
        rebalance(partitions.asJava)
        val beginningOffset = Map(partitions.head -> java.lang.Long.valueOf(0L)).asJava
        mockConsumer.updateBeginningOffsets(beginningOffset)
      }
    }
  }
}
