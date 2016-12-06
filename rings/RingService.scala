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
 * @param burstSize number of commands per burst
 */

class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, numReplica: Int, numRead: Int, numWrite: Int) extends Actor {
  val generator = new scala.util.Random
  val KVClient = new KVClient(storeServers, numReplica, numRead, numWrite)
  val dirtycells = new AnyMap
  val localWeight: Int = 70
  val log = Logging(context.system, this)

  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None


  def receive() = {
      case TestRead(key) =>
        tRead(key)
      case TestWrite(key, value) =>
        tWrite(key, value)
      case View(e) =>
        endpoints = Some(e)
  }
  private def tRead(key: BigInt): Int = {
    val ret = KVClient.directRead(key)
    return ret
  }
  private def tWrite(key: BigInt, value: Any) = {
    KVClient.directWrite(key, value)
  }


}

object RingServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, numReplica: Int, numRead: Int, numWrite: Int): Props = {
    Props(classOf[RingServer], myNodeID, numNodes, storeServers, burstSize, numReplica, numRead, numWrite)
  }
}
