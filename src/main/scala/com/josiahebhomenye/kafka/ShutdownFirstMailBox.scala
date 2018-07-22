package com.josiahebhomenye.kafka

import akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import KafkaCoordinator._
import akka.actor.{ActorSystem, PoisonPill}
import com.josiahebhomenye.kafka.ConsumerActor.PollNext
import com.typesafe.config.Config



class ShutdownFirstMailBox(settings: ActorSystem.Settings, config: Config ) extends UnboundedStablePriorityMailbox(
  PriorityGenerator{
    case Shutdown | ShutDownPending | ShutDownComplete => 0
    case PoisonPill => 2
    case PollNext => 3
    case _ => 1
  }
){

}
