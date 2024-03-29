package rings

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
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

class RingServer (val myNodeID: Int,  val storeTable: scala.collection.mutable.HashMap[Int, Int], val numNodes: Int, storeServers: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int, system: ActorSystem) extends Actor {
  val generator = new scala.util.Random
  val KVClient = new KVClient(myNodeID, storeTable, storeServers, numReplica, numRead, numWrite, numStore)
  val log = Logging(context.system, this)
  var endpoints: Option[Seq[ActorRef]] = None
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  val statTable = new scala.collection.mutable.HashMap[BigInt, Stat]
  def receive() = {
      case CheckReport() =>
        sender() ! statTable
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
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSUCCESS: client ${myNodeID} read key: ${key}, value: ${ret.value}, version: ${ret.versionNum}\033[0m")
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: client ${myNodeID} read key: ${key}, not enough success read \033[0m")
      println(s"client ${myNodeID} read failed")
    }
  }

  private def tWrite(key: BigInt, value: Int) = {
    if (!statTable.contains(key)) {
      statTable.put(key, new Stat(key, 0, 0))
    }
    val ret = KVClient.directWrite(key, value)
    if (ret.status == 0) {
      statTable(key).successWrite += 1
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSUCCESS: client ${myNodeID} write key: ${key}, value: ${value}\033[0m")
    } else if (ret.status == 1) {
      statTable(key).failWrite += 1
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: client ${myNodeID} write key: ${key}, value: ${value}, version check failed\033[0m")
    } else {
      statTable(key).failWrite += 1
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: client ${myNodeID} write key: ${key}, value: ${value}, number of success write not enough\033[0m")
    }
  }


}

object RingServer {
  def props(myNodeID: Int,  storeTable: scala.collection.mutable.HashMap[Int, Int], numNodes: Int, storeServers: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int, system: ActorSystem): Props = {
    Props(classOf[RingServer], myNodeID, storeTable, numNodes, storeServers, numReplica, numRead, numWrite, numStore, system)
  }
}
