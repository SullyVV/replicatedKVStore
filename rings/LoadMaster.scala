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
    /********* read non-existed key *********/
//    servers(0) ! TestRead(1)
    /****************************************/

    /********* Single Client, single key, multiple write *********/
//    servers(0) ! TestWrite(2,1)
//    servers(0) ! TestWrite(2,2)
//    servers(0) ! TestWrite(2,3)
//    servers(0) ! TestWrite(2,4)
//    servers(0) ! TestWrite(2,5)
//    servers(0) ! TestWrite(2,6)
//    servers(0) ! TestRead(2)
    /*************************************************************/

    /********* multiple client, same key, multple write (write order) *********/
//    servers(0) ! TestWrite(1,1)
//    Thread.sleep(20)
//    servers(1) ! TestWrite(1,2)
//    Thread.sleep(20)
//    servers(2) ! TestWrite(1,3)
//    Thread.sleep(20)
//    servers(0) ! TestRead(1)
    /*************************************************************************/

    /********* multiple client, same key, multple write (random order) *********/
//    servers(0) ! TestWrite(1,1)
//    servers(1) ! TestWrite(1,2)
//    servers(2) ! TestWrite(1,3)
//    servers(0) ! TestRead(1)
    /*************************************************************************/

    /********* multiple client, multiple key, multple write *********/
//    servers(0) ! TestWrite(61,1)
//    servers(1) ! TestWrite(83,2)
//    servers(2) ! TestWrite(1231,3)
//    servers(0) ! TestRead(1231)
//    servers(1) ! TestRead(61)
//    servers(2) ! TestRead(83)
    /****************************************************************/

    /********* write with old version number arrives later than new write *********/
//    servers(0) ! TestWrite(1, 5)
//    Thread.sleep(20)
//    servers(1) ! TestWrite(1, 10)
//    Thread.sleep(50)
//    servers(2) ! TestRead(1)
    /******************************************************************************/

    /********* partial success read and partial success write *********/
//    println(s"******************first write******************")
//    println()
//    servers(0) ! TestWrite(2, 1)
//    Thread.sleep(30)
//    servers(2) ! TestRead(2)
//    Thread.sleep(30)
//    println()
//    println(s"******************second write******************")
//    println()
//    servers(1) ! TestWrite(2,2)
//    Thread.sleep(30)
//    servers(2) ! TestRead(2)
    /******************************************************************/

    /********* store Server offline and come back online *********/
    stores(4) ! Disconnect()
    servers(0) ! TestWrite(2,1)
    Thread.sleep(20)
    servers(0) ! TestRead(2)
    Thread.sleep(20)
    stores(4) ! Connect()
    Thread.sleep(10)
    servers(0) ! TestRead(2)
    /******************************************************************/

  }
}

object LoadMaster {
   def props(numNodes: Int, servers: Seq[ActorRef], stores: Seq[ActorRef]): Props = {
      Props(classOf[LoadMaster], numNodes, servers, stores)
   }
}

