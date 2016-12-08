package rings

import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (myNodeID: Int, stores: Seq[ActorRef], numReplica: Int, numRead: Int, numWrite: Int, numStore: Int) {
  implicit val timeout = Timeout(100 seconds)
  private val generator = new scala.util.Random()
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  def directRead(key: BigInt): ReturnData = {
    // this random Store function as the receptionist for this
    val receptionStore = generator.nextInt(10)
    val hashedKey = hashForKey(key).toInt
    val future = ask(stores(receptionStore), RouteMsg(0, key, hashedKey, -1, -1))
    val done = Await.result(future, timeout.duration).asInstanceOf[ReturnData]
    return done
  }

  def directWrite(key: BigInt, value: Int) {
    val versionNum = System.nanoTime()
    // test race condition
//    if (key == 2 && myNodeID == 0) {
//      Thread.sleep(50)
//    }
    // this random Store function as the receptionist for this
    val receptionStore = generator.nextInt(numStore)
    val hashedKey = hashForKey(key).toInt
    val future = ask(stores(receptionStore), RouteMsg(1, key, hashedKey, value, versionNum))
    val done = Await.result(future, timeout.duration).asInstanceOf[ReturnData]
    if (done.status == 0) {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSUCCESS: client ${myNodeID} write key: ${key}, value: ${value}, version: ${versionNum}\033[0m")
    } else if (done.status == 1) {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: client ${myNodeID} write key: ${key}, value: ${value}, version: ${versionNum}, version check failed\033[0m")
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFAIL: client ${myNodeID} write key: ${key}, value: ${value}, version: ${versionNum}, number of success write not enough\033[0m")
    }
  }

  import java.security.MessageDigest

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

}
