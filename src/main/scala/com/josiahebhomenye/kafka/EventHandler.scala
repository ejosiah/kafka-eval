package com.josiahebhomenye.kafka

import kafka.common.OffsetMetadata
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

case class Event[T](key: String, value: T)

trait EventHandler[T] {

  def group: String

  def topic: String

  def deserializer: Class[_]

  def onCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Unit] = Future.successful()

  def on(events: Seq[Event[T]]): Future[Unit]

  private[kafka] def onTypeLess(event: Seq[Event[AnyRef]]): Future[Unit] = {
    on(event.asInstanceOf[Seq[Event[T]]])
  }

  private[kafka] def convert(record: ConsumerRecord[String, AnyRef]): Event[AnyRef] = {
    Event(record.key(), record.value())
  }
}
