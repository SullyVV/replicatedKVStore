package rings



import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global



sealed trait KVStoreAPI
case class Put(key: BigInt, value: Int, versionNum: Int) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
case class Upadate(key: BigInt, value: Int, versionNum: Int) extends KVStoreAPI
case class Disconnect() extends KVStoreAPI
case class Connect() extends KVStoreAPI
case class TmpPut(key: BigInt, value: Int, versionNum: Int, originHolder: Int) extends KVStoreAPI
case class Transfer(key: BigInt, value: Int, versionNum: Int) extends KVStoreAPI
case class AcquireVersionNum(key: BigInt) extends KVStoreAPI
class StoredData(var key: BigInt, var value: Int, var versionNum: Int)
// status: 0 -> write success, 1 -> version check failed, 2 -> minimum number
class ReturnData(val status: Int, var key: BigInt, var value: Int, var versionNum: Int)
// status: 0 --> read success, 1 --> read failure, 2 --> offline
class ReadData(val status: Int, var myStoreID: Int, var key: BigInt, var value: Int, var versionNum: Int)

/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore (val myStoreID: Int, val system: ActorSystem) extends Actor {
  implicit val timeout = Timeout(10 seconds)
  private val store = new scala.collection.mutable.HashMap[BigInt, StoredData]
  private val tmpList = new mutable.HashMap[Int, StoredData]
  val generator = new scala.util.Random
  var endpoints: Option[Seq[ActorRef]] = None
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  private var offLine = false
  // periodically check the tmpList, transfer back important data
  system.scheduler.schedule(15 milliseconds,15  milliseconds) {
    transfer()
  }
  override def receive = {
    case AcquireVersionNum(key) =>
      if (offLine == true) {
        sender() ! -1
      } else {
        if (!store.contains(key)) {
          sender() ! 0
        } else {
          sender() ! store(key).versionNum
        }
      }

    case Disconnect() =>
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[33m  OFFLINE: storeServer ${myStoreID} is offline\033[0m")
      offLine = true

    case Connect() =>
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[34m  ONLINE: storeServer ${myStoreID} is online\033[0m")
      offLine = false

    case View(e) =>
      endpoints = Some(e)

    case Transfer(key, value, versionNum) =>
      if (offLine == true) {
        println(s"storeServer ${myStoreID} get transfer data ${key}, but it is still offline so ignore it")
        sender() ! 1
      } else {
        //update itself
        println(s"storeServer ${myStoreID} get transfer data ${key}, it is online now so update its own store")
        if (!(store.contains(key) && versionNum <= store(key).versionNum)) {
          store.put(key, new StoredData(key, value, versionNum))
        }
        sender() ! 0
      }

    case Upadate(key, value, versionNum) =>
      update(key, value, versionNum)

    case Put(key, value, versionNum) =>
      // ignore the write failure at this moment, now only reason unable to write is version check failure
      // return integer: 0 -> write success, 1 -> version check failed, 2 -> offline
      if (offLine == true) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31m    FAIL: storeServer ${myStoreID} is offline\033[0m")
        sender() ! 2
      } else {
        if (store.contains(key) && versionNum <= store(key).versionNum) {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31m    FAIL: storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
          sender() ! 1
        } else {
          // write success
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[34m    SUCCESS: storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}\033[0m")
          store.put(key, new StoredData(key, value, versionNum))
          sender() ! 0
        }
      }

    case TmpPut(key, value, versionNum, originHolder) =>
      if (offLine == true) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31m    FAIL: tmp storeServer ${myStoreID} is offline\033[0m")
        sender() ! 2
      } else {
        if (store.contains(key) && versionNum <= store(key).versionNum) {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31m    FAIL: tmp storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
          sender() ! 1
        } else {
          // write success
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[34m    SUCCESS: tmp storeServer ${myStoreID} write key: ${key}, value: ${value}, version: ${versionNum}\033[0m")
          store.put(key, new StoredData(key, value, versionNum))
          tmpList.put(originHolder, new StoredData(key, value, versionNum))
          sender() ! 0
        }
      }

    case Get(key) =>
      if (offLine == true) {
        sender() ! new ReadData(2, myStoreID, key, -1, -1)
      } else {
        if (store.contains(key)) {
          sender() ! new ReadData(0, myStoreID, key, store(key).value, store(key).versionNum)
        } else {
          sender() ! new ReadData(1, myStoreID, key, -1, -1)
        }
      }
  }
  def transfer() {
    val storeServers = endpoints.get
    for ((k,v) <- tmpList) {
      val future = ask(storeServers(k), Transfer(v.key, v.value, v.versionNum))
      val done = Await.result(future, timeout.duration).asInstanceOf[Int]
      if (done == 0) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[36mAUTOMATIC: storeServer ${myStoreID} transfer key ${v.key} from to original store server ${k} successful\033[0m")
        println(s"")
        // means original server is online and get the msg, clean up this tmp server's store and tmpList
        tmpList.remove(k)
        store.remove(v.key)
      }
    }
  }
  def update(key: BigInt, value: Int, versionNum: Int) {
    if (store.contains(key)) {
      store(key).value = value
      store(key).versionNum = versionNum
    } else {
      store.put(key, new StoredData(key, value, versionNum))

    }
  }
}


object KVStore {
  def props(myStoreID: Int, system: ActorSystem): Props = {
    Props(classOf[KVStore], myStoreID, system)
  }
}
