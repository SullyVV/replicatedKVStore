package rings

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.immutable.HashMap

class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (stores: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int) {
  private val cache = new AnyMap
  implicit val timeout = Timeout(5 seconds)
  private val storeTable = new scala.collection.mutable.HashMap[Int,Int]
  import scala.concurrent.ExecutionContext.Implicits.global
  /** initialize storeTable **/
  //0 ~ 9 -> store1, 10 ~ 19 -> store 2, 20 ~ 29 -> store 3, 30 ~ 39 -> store 4, 40 ~ 49 -> store 5
  for (i <- 0 until 10) {
    storeTable.put(i, 0)
  }
  for (i <- 10 until 20) {
    storeTable.put(i, 1)
  }
  for (i <- 20 until 30) {
    storeTable.put(i, 2)
  }
  for (i <- 30 until 40) {
    storeTable.put(i, 3)
  }
  for (i <- 40 until 50) {
    storeTable.put(i, 4)
  }

  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt): Int = {
    //println(s"hash code is ${key.hashCode()} and number of unit circle is ${Math.abs(key.hashCode()%50)}, version number can be ${System.nanoTime()}")
    val
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Any) = {
    // we used consistent hashing here, a certain range of keys -> a store server, and replicated in the following n-1 store servers.
    val keyPosition = Math.abs(key.hashCode()%50)
    // 0 ~ 9 -> store1, 10 ~ 19 -> store 2, 20 ~ 29 -> store 3, 30 ~ 39 -> store 4, 40 ~ 49 -> store 5
    val versionNum = System.nanoTime()
    for (i <- 0 until numReplica) {
      // w = 3
      var curr = storeTable(keyPosition) + i
      if (curr > 4) {
        curr -= 5
      }
      val future = ask(stores(curr), Put(key,value, versionNum))
      val done = Await.result(future, timeout.duration).asInstanceOf[Option[Any]]
    }
  }

  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    */
  def hashForKey(nodeID: Int, cellSeq: Int): BigInt = {
    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ cellSeq.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }
}
