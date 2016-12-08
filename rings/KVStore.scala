package rings

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask

import scala.concurrent.Await
import akka.util.Timeout

import scala.concurrent.duration._

sealed trait KVStoreAPI
case class Put(key: BigInt, value: Int, versionNum: Long, preferenceList: scala.collection.mutable.ArrayBuffer[Int]) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
case class Upadate(key: BigInt, value: Int, versionNum: Long) extends KVStoreAPI
case class RouteMsg(operation: Int, key: BigInt, hashedKey: Int, value: Int, versionNum: Long) extends KVStoreAPI

class StoredData(var key: BigInt, var value: Int, var versionNum: Long)
// status: 0 -> write success, 1 -> version check failed, 2 -> minimum number
class ReturnData(val status: Int, var key: BigInt, var value: Int, var versionNum: Long)
// status: 0 --> read success, 1 --> read failure
class ReadData(val status: Int, var myStoreID: Int, var key: BigInt, var value: Int, var versionNum: Long)
/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore (val myStoreID: Int, val storeTable: scala.collection.mutable.HashMap[Int, Int], val numStore: Int, val numReplica: Int, val numRead: Int, val numWrite: Int) extends Actor {
  implicit val timeout = Timeout(10 seconds)
  private val store = new scala.collection.mutable.HashMap[BigInt, StoredData]
  val generator = new scala.util.Random
  var endpoints: Option[Seq[ActorRef]] = None
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  override def receive = {
    case View(e) =>
      endpoints = Some(e)
    case Upadate(key, value, versionNum) =>
      update(key, value, versionNum)
    case Put(key, value, versionNum, preferenceList) =>
      // ignore the write failure at this moment, now only reason unable to write is version check failure
      // return integer: 0 -> write success, 1 -> version check failed, 2 -> write failed
      if (store.contains(key) && versionNum < store(key).versionNum) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31m    FAIL: storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
        sender() ! 1
      } else {
        // write success
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[34m    SUCCESS: storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}\033[0m")
        store.put(key, new StoredData(key, value, versionNum))
        sender() ! 0
      }
    case Get(key) =>
      if (store.contains(key)) {
        sender() ! new ReadData(0, myStoreID, key, store(key).value, store(key).versionNum)
      } else {
        sender() ! new ReadData(1, myStoreID, key, -1, -1)
      }
  }

  def update(key: BigInt, value: Int, versionNum: Long) {
    if (store.contains(key)) {
      store(key).value = value
      store(key).versionNum = versionNum
    } else {
      store.put(key, new StoredData(key, value, versionNum))

    }
  }
}


object KVStore {
  def props(myStoreID: Int, storeTable: scala.collection.mutable.HashMap[Int, Int], numStore: Int, numReplica: Int, numRead: Int, numWrite:Int): Props = {
    Props(classOf[KVStore], myStoreID, storeTable, numStore, numReplica, numRead, numWrite)
  }
}
