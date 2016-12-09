package rings

import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import java.security.MessageDigest
/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (myNodeID: Int,  val storeTable: scala.collection.mutable.HashMap[Int, Int], stores: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int) {
  implicit val timeout = Timeout(100 seconds)
  private val generator = new scala.util.Random()
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  def directRead(key: BigInt): ReturnData = {
    val hashedKey = hashForKey(key).toInt
    val startStoreServer = findStoreServer(hashedKey)
    val preferenceList = new scala.collection.mutable.ArrayBuffer[Int]
    for (i <-0 until numReplica) {
      preferenceList += (startStoreServer + i)%numStore
    }
    println(s"preference list for read key ${key} is ${preferenceList}")
    val readList = new scala.collection.mutable.ArrayBuffer[ReadData]
    val updateList = new scala.collection.mutable.ArrayBuffer[Int]
    // first, read from all replicas
    for (i <- 0 until numReplica) {
      val future = ask(stores(preferenceList(i)), Get(key))
      val done = Await.result(future, timeout.duration).asInstanceOf[ReadData]
      readList += done
    }
    // check number of successful read, find read result with highest version number
    var rCnt = 0
    var readVNum = Long.MinValue
    var readValue = -1
    for (readElement <- readList) {
      if (readElement.status == 0) {
        rCnt += 1
        if (readElement.versionNum > readVNum) {
          readVNum = readElement.versionNum
          readValue = readElement.value
        }
      }
    }
    println(s"number of success read is: ${rCnt}")
    if (rCnt < numRead) {
      return new ReturnData(2, key, -1, -1)
    }
    val ret = new ReturnData(0, key, readValue, readVNum)
    // update store servers with stale data
    for (readElement <- readList) {
      if (readElement.versionNum < ret.versionNum) {
        stores(readElement.myStoreID) ! Upadate(ret.key, ret.value, ret.versionNum)
      }
    }
    return ret
  }

  def directWrite(key: BigInt, value: Int): ReturnData = {
    val versionNum = System.nanoTime()
    val hashedKey = hashForKey(key).toInt
    // client act as the coordinator
    val startStoreServer = findStoreServer(hashedKey)
    val preferenceList = new scala.collection.mutable.ArrayBuffer[Int]
    for (i <-0 until numReplica) {
      preferenceList += (startStoreServer + i)%numStore
    }
    println(s"preference list for write key ${key} is ${preferenceList}")
    var wCnt = 0
    for (i <- 0 until preferenceList.size) {
      val future = ask(stores(preferenceList(i)), Put(key, value, versionNum, preferenceList))
      val done = Await.result(future, timeout.duration).asInstanceOf[Int]
      if (done == 1) {
        // this write is already outdated
        return new ReturnData(1, key, value, versionNum)
      } else if (done == 2){
          // specified store server is offline, try the backup one
          println(s"store server ${preferenceList(i)} is offline, rewrite to storeServer ${(preferenceList(i)+numReplica)%numStore}")
          val future = ask(stores((preferenceList(i)+numReplica)%numStore), TmpPut(key, value, versionNum, preferenceList(i)))
          val done = Await.result(future, timeout.duration).asInstanceOf[Int]
          if (done == 0) {
            wCnt += 1
          }
      } else {
          wCnt += 1
      }
    }

    println(s"number of success write for key: ${key}, value: ${value} is: ${wCnt}")
    if (wCnt >= numWrite) {
      // write success
      return new ReturnData(0, key, value, versionNum)
    } else {
      // write failed due to unable to write
      return new ReturnData(2, key, value, versionNum)
    }
  }

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    */
  def hashForKey(key: BigInt): BigInt = {
    val label =key.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }
  def findStoreServer(hashedKey: Int): Int = {
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

}
