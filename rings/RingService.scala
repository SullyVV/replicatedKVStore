package rings

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

class RingCell(var prev: BigInt, var next: BigInt)
class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]

/**
 * RingService is an example app service for the actor-based KVStore/KVClient.
 * This one stores RingCell objects in the KVStore.  Each app server allocates new
 * RingCells (allocCell), writes them, and reads them randomly with consistency
 * checking (touchCell).  The allocCell and touchCell commands use direct reads
 * and writes to bypass the client cache.  Keeps a running set of Stats for each burst.
 *
 * @param myNodeID sequence number of this actor/server in the app tier
 * @param numNodes total number of servers in the app tier
 * @param storeServers the ActorRefs of the KVStore servers
 */

class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int, system: ActorSystem) extends Actor {
  val generator = new scala.util.Random
  val KVClient = new KVClient(myNodeID, storeServers, numReplica, numRead, numWrite, numStore)
  val log = Logging(context.system, this)
  var endpoints: Option[Seq[ActorRef]] = None

  def receive() = {
      case TestRead(key) =>
        tRead(key)
      case TestWrite(key, value) =>
        tWrite(key, value)
      case View(e) =>
        endpoints = Some(e)
  }

  private def tRead(key: BigInt) {
    val ret = KVClient.directRead(key)
    if (ret.status == 0) {
      println(s"client ${myNodeID} read successful, key ${key}, value ${ret.value}")
    } else {
      println(s"client ${myNodeID} read failed")
    }
  }

  private def tWrite(key: BigInt, value: Int) = {
    KVClient.directWrite(key, value)
  }


}

object RingServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int, system: ActorSystem): Props = {
    Props(classOf[RingServer], myNodeID, numNodes, storeServers, numReplica, numRead, numWrite, numStore, system)
  }
}
