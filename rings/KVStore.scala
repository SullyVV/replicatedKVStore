package rings

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._

sealed trait KVStoreAPI
case class Put(key: BigInt, value: Int, versionNum: Long, preferenceList: scala.collection.mutable.ArrayBuffer[Int]) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
case class Upadate(key: BigInt, value: Int, versionNum: Long) extends KVStoreAPI
class StoredData(var value: Int, var versionNum: Long, preferenceList: scala.collection.mutable.ArrayBuffer[Int])
// status: 0 -> write success, 1 -> version check failed, 2 -> write failed
class ReturnData(val status: Integer, var key: BigInt, var value: Int, var versionNum: Long)
class ReadData(var myStoreID: Int, var key: BigInt, var value: Int, var versionNum: Long)
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

  override def receive = {
    case View(e) =>
      endpoints = Some(e)
    case RouteMsg(operation, key, hashedKey, value, versionNum) =>
      val stores = endpoints.get
      if (operation == 1) {
        val res = write(key, hashedKey, value, versionNum, stores)
        sender() ! res
      } else {
        val res = read(key, hashedKey, stores)
        sender() ! res
      }

    case Upadate(key, value, versionNum) =>
      store(key).value = value
      store(key).versionNum = versionNum
    case Put(key, value, versionNum, preferenceList) =>
      // ignore the write failure at this moment, now only reason unable to write is version check failure
      //val rand = generator.nextInt(100)
      // return integer: 0 -> write success, 1 -> version check failed, 2 -> write failed
      if (store.contains(key) && versionNum < store(key).versionNum) {
        println(s" version check failed")
        sender() ! 1
      } else {
        // write success
        store.put(key, new StoredData(value, versionNum, preferenceList))
        sender() ! 0
      }
    case Get(key) =>
      if (store.contains(key)) {

      } else {

      }
  }

  def findCoordinator(hashedKey: Int): Int = {
    var tmpKey = -1
    var min = Int.MaxValue
    for ((k, v) <- storeTable) {
      if (Math.abs(k - hashedKey) < min) {}
      tmpKey = k
      min = Math.abs(k - hashedKey)
    }
    return tmpKey
  }
  def read(key: BigInt, hashedKey: Int, value: Int, stores: Seq[ActorRef]): ReturnData = {
    val coordinatorNum = findCoordinator(hashedKey)
    val preferenceList = new scala.collection.mutable.ArrayBuffer[Int]
    for (i <- 0 until numReplica) {
      preferenceList += (coordinatorNum + i)%numStore
    }
    val readList = new scala.collection.mutable.ArrayBuffer[ReadData]
    val updateList = new scala.collection.mutable.ArrayBuffer[Int]
    // first, read from all replcias
    for (i <- 0 until numReplica) {
      if (preferenceList(i) == myStoreID) {
        if (store.contains(key)) {
          readList += new ReadData(storeID, key, store(key).value, store(key).versionNum)
        }
      } else {
        val future = ask(stores(preferenceList(i)), Get(key))
        val done = Await.result(future, timeout.duration).asInstanceOf[ReadData]
        readList += done
      }
    }
    // check number of successful read
    var currNode = readList(0).myStoreID
    val ret = new ReturnData(0, key, readList(0).value, readList(0).versionNum)
    for (i <- 1 until numReplica) {
      if (readList(i).versionNum > ret.versionNum) {
        updateList += currNode
        currNode = readList(i).myStoreID
        ret.value = readList(i).value
        ret.versionNum = readList(i).versionNum
      }
    }
    // remind stores with stale data
    for (storeID <- updateList) {
      stores(storeID) ! Update(ret.key, ret.value, ret.versionNum)
    }
  }
  def write(key: BigInt, hashedKey: Int, value: Int, versionNum: Long, stores: Seq[ActorRef]): ReturnData = {
    val coordinatorNum = findCoordinator(hashedKey)
    val preferenceList = new scala.collection.mutable.ArrayBuffer[Int]
    for (i <- 0 until numReplica) {
      preferenceList += (coordinatorNum + i)%numStore
    }
    var cnt = 0
    for (i <- 0 until numReplica) {
      if (preferenceList(i) == myStoreID) {
        if (store.contains(key) && versionNum < store(key).versionNum) {
          // once version check failed, return write failure directly
          return new ReturnData(1, key, value, versionNum)
        } else {
          // write success
          store.put(key, new StoredData(value, versionNum, preferenceList))
          cnt += 1
        }
      } else {
        val future = ask(stores(preferenceList(i)), Put(key, value, versionNum, preferenceList))
        val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        if (done == true) {
          cnt += 1
        } else {
          return new ReturnData(1, key, value, versionNum)
        }
      }
    }
    if (cnt >= numWrite) {
      return new ReturnData(0, key, value, versionNum)
    } else {
      return new ReturnData(2, key, value, versionNum)
    }
  }
}


object KVStore {
  def props(myStoreID: Int, storeTable: scala.collection.mutable.HashMap[Int, Int], numStore: Int, numReplica: Int, numRead: Int, numWrite:Int): Props = {
     Props(classOf[KVStore], myStoreID, storeTable, numStore, numReplica, numRead, numWrite)
  }
}
