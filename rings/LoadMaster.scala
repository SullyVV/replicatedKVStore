package rings

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

sealed trait LoadMasterAPI
case class Start() extends LoadMasterAPI
case class BurstAck(senderNodeID: Int, stats: Stats) extends LoadMasterAPI
case class Join() extends LoadMasterAPI

/** LoadMaster is a singleton actor that generates load for the app service tier, accepts acks from
  * the app tier for each command burst, and terminates the experiment when done.  It uses the incoming
  * acks to self-clock the flow of commands, to avoid swamping the mailbox queues in the app tier.
  * It also keeps running totals of various Stats returned by the app servers with each burst ack.
  * A listener may register via Join() to receive a message when the experiment is done.
  *
  * @param numNodes How many actors/servers in the app tier
  * @param servers ActorRefs for the actors/servers in the app tier
  */

class LoadMaster (val numNodes: Int, val servers: Seq[ActorRef], stores: Seq[ActorRef]) extends Actor {
  val log = Logging(context.system, this)
  var active: Boolean = true
  var listener: Option[ActorRef] = None
  var nodesActive = numNodes
  var maxPerNode: Int = 0

  val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case Start() =>
      log.info("Master starting bursts")
      test()
      sender() ! true
    case Join() =>
      listener = Some(sender)
  }

  def test() = {
    // test for write
    servers(0) ! TestWrite(2,1)
    //servers(1) ! TestWrite(5,2)
    Thread.sleep(10)
    servers(1) ! TestWrite(2, 5)
    Thread.sleep(10)
    servers(2) ! TestWrite(18, 7)
    servers(1) ! TestWrite(19, 20)
    servers(0) ! TestWrite(290, 5)
    servers(0) ! TestWrite(2, 15)
    //servers(2) ! TestWrite(2,2)
    // test for read
    //Thread.sleep(30)
    servers(2) ! TestRead(2)
    servers(0) ! TestRead(18)
    servers(1) ! TestRead(19)

    //Thread.sleep(20)
    //servers(1) ! TestWrite(2,3)
    //servers(1) ! TestRead(2)
    //Thread.sleep(10)
    //servers(1) ! TestWrite(2,2)
//    Thread.sleep(10)
//    servers(1) ! TestWrite(2,2)
    //servers(0) ! TestWrite(2,1)
  }
}

object LoadMaster {
   def props(numNodes: Int, servers: Seq[ActorRef], stores: Seq[ActorRef]): Props = {
      Props(classOf[LoadMaster], numNodes, servers, stores)
   }
}

