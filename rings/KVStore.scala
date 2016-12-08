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
      update(key, value, versionNum)
    case Put(key, value, versionNum, preferenceList) =>
      // ignore the write failure at this moment, now only reason unable to write is version check failure
      // return integer: 0 -> write success, 1 -> version check failed, 2 -> write failed
      if (store.contains(key) && versionNum < store(key).versionNum) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
        sender() ! 1
      } else {
        // write success
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

  def findCoordinator(hashedKey: Int): Int = {
    val posOfKey = Math.abs(hashedKey%360)
    var tmpKey = -1
    var min = Int.MaxValue
    for ((k, v) <- storeTable) {
      if (k > posOfKey && Math.abs(k - posOfKey) < min) {
        tmpKey = v
        min = Math.abs(k - posOfKey)
      }
    }
    return tmpKey
  }
  def update(key: BigInt, value: Int, versionNum: Long) {
    if (store.contains(key)) {
      store(key).value = value
      store(key).versionNum = versionNum
    } else {
      store.put(key, new StoredData(key, value, versionNum))

    }
  }
  def read(key: BigInt, hashedKey: Int, stores: Seq[ActorRef]): ReturnData = {
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
          readList += new ReadData(0, myStoreID, key, store(key).value, store(key).versionNum)
        }
      } else {
        val future = ask(stores(preferenceList(i)), Get(key))
        val done = Await.result(future, timeout.duration).asInstanceOf[ReadData]
        readList += done
      }
    }
    // check number of successful read, find read result with heighest version number
    var cnt = 0
    var readVNum = Long.MinValue
    var readValue = -1
    for (readElement <- readList) {
      if (readElement.status == 0) {
        cnt += 1
        if (readElement.versionNum > readVNum) {
          readVNum = readElement.versionNum
          readValue = readElement.value
        }
      }
    }
    if (cnt < numRead) {
      return new ReturnData(2, key, -1, -1)
    }
    val ret = new ReturnData(0, key, readValue, readVNum)
    // get those store servers who holds this key and needs an update
    for (readElement <- readList) {
      if (readElement.versionNum < ret.versionNum) {
        updateList += readElement.myStoreID
      }
    }
    // multicast this update info to  all involved store servers
    for (storeServers <- updateList) {
      stores(storeServers) ! Upadate(ret.key, ret.value, ret.versionNum)
    }
    return ret
  }

  def write(key: BigInt, hashedKey: Int, value: Int, versionNum: Long, stores: Seq[ActorRef]): ReturnData = {
    // try to write to all replicas, w success write --> success
    val coordinatorNum = findCoordinator(hashedKey)
    val preferenceList = new scala.collection.mutable.ArrayBuffer[Int]
    for (i <- 0 until numReplica) {
      preferenceList += (coordinatorNum + i)%numStore
    }
    println(s"storeServer ${myStoreID} is the receptionStoreServer, pList is ${preferenceList}")
    var cnt = 0
    for (i <- 0 until preferenceList.size) {
      // test r/w/n
//      if (key == 2 && preferenceList(i) == 6) {
//        println("pass this")
//      }
      //else
      if (preferenceList(i) == myStoreID) {
        // receptionStore is in preference list
        if (store.contains(key) && versionNum < store(key).versionNum) {
          // once version check failed, return write failure directly
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: storeServer ${preferenceList(i)} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
          return new ReturnData(1, key, value, versionNum)
        } else {
          // write success
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSuccess\033[0m: storeServer ${preferenceList(i)} write \033[45mkey: ${key}, value: ${value}, version: ${versionNum}\033[0m success")
          store.put(key, new StoredData(key, value, versionNum))
          cnt += 1
        }
      } else {
        val future = ask(stores(preferenceList(i)), Put(key, value, versionNum, preferenceList))
        val done = Await.result(future, timeout.duration).asInstanceOf[Int]
        if (done == 0) {
          // send back 0 --> write success
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSuccess\033[0m: storeServer ${preferenceList(i)} write \033[45mkey: ${key}, value: ${value}, version: ${versionNum}\033[0m success")
          cnt += 1
        } else if (done == 1){
          // send back 1 --> version check failed
          return new ReturnData(1, key, value, versionNum)
        }
      }
    }
    println(s"number of success write for key: ${key} is: ${cnt}")
    if (cnt >= numWrite) {
      // write success
      return new ReturnData(0, key, value, versionNum)
    } else {
      // write failed due to unable to write
      return new ReturnData(2, key, value, versionNum)
    }
  }
}


object KVStore {
  def props(myStoreID: Int, storeTable: scala.collection.mutable.HashMap[Int, Int], numStore: Int, numReplica: Int, numRead: Int, numWrite:Int): Props = {
    Props(classOf[KVStore], myStoreID, storeTable, numStore, numReplica, numRead, numWrite)
  }
}
