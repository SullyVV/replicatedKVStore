package rings

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]
class ReadData(var success: Boolean, var value: Int)

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (myNodeID: Int, stores: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int) {
  implicit val timeout = Timeout(5 seconds)
  private val storeTable = new scala.collection.mutable.HashMap[Int,Int]
  import scala.concurrent.ExecutionContext.Implicits.global
  /** initialize storeTable **/
  //0 ~ 9 -> store1, 10 ~ 19 -> store 2, 20 ~ 29 -> store 3, 30 ~ 39 -> store 4, 40 ~ 49 -> store 5
  for (i <- 0 until 100) {
    storeTable.put(i, (i/10))
  }

  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt): ReadData = {
    //println(s"hash code is ${key.hashCode()} and number of unit circle is ${Math.abs(key.hashCode()%50)}, version number can be ${System.nanoTime()}")
    val hashedKey = hashForKey(key).toInt
    val keyPosition = Math.abs(hashedKey%100)
    val list = new scala.collection.mutable.ArrayBuffer[StoredData]
    var cnt = 0
    for (i <- 0 until numReplica) {
      var curr = storeTable(keyPosition) + i
      if (curr > numStore - 1) {
        curr -= numStore
      }
      val future = ask(stores(curr), Get(key))
      val done = Await.result(future, timeout.duration).asInstanceOf[ReturnData]
      if (done.success == true) {
        list += new StoredData(done.value, done.versionNum)
        cnt += 1
      }
    }
    if (cnt >= numRead) {
      var ret = list(0).value
      var newstVN = list(0).versionNum
      for (i <- 1 until list.size) {
        if (list(i).versionNum > newstVN) {
          newstVN = list(i).versionNum
          ret = list(i).value
        }
      }
      return new ReadData(true, ret)
    } else {
      // retry
      return new ReadData(false, -1)
    }
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Int): Boolean = {
    // we used consistent hashing here, a certain range of keys -> a store server, and replicated in the following n-1 store servers.
    val hashedKey = hashForKey(key).toInt
    val keyPosition = Math.abs(hashedKey%100)
    // 0 ~ 9 -> store1, 10 ~ 19 -> store 2, 20 ~ 29 -> store 3, 30 ~ 39 -> store 4, 40 ~ 49 -> store 5
    val versionNum = System.nanoTime()
    println(s"this is client ${myNodeID}, version number is ${versionNum}")
    if (myNodeID == 0) {
      println(s"this is app 0, sleep for a while")
      Thread.sleep(10)
    }
    var cnt = 0
    for (i <- 0 until numReplica) {
      var curr = storeTable(keyPosition) + i
      if (curr > numStore - 1) {
        curr -= numStore
      }
      val future = ask(stores(curr), Put(myNodeID, key,value, versionNum))
      val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
      if (done == true) {
         cnt += 1
      }
    }
    if (cnt >= numWrite) {
      return true
    } else {
      // retry
      return false
    }
  }

  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    */
//  def hashForKey(nodeID: Int, cellSeq: Int): BigInt = {
//    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ cellSeq.toString
//    val md: MessageDigest = MessageDigest.getInstance("MD5")
//    val digest: Array[Byte] = md.digest(label.getBytes)
//    BigInt(1, digest)
//  }
  def hashForKey(key: BigInt): BigInt = {
    val label =key.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }

}
