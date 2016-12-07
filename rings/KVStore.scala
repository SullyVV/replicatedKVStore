package rings

import akka.actor.{Actor, Props}

sealed trait KVStoreAPI
case class Put(myNodeID: Int, key: BigInt, value: Int, versionNum: Long) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
class StoredData(var value: Int, var versionNum: Long)
class ReturnData(val success: Boolean, var value: Int, var versionNum: Long)
/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore extends Actor {
  private val store = new scala.collection.mutable.HashMap[BigInt, StoredData]
  val generator = new scala.util.Random
  override def receive = {
    case Put(myNodeID, key, value, versionNum) =>
      val rand = generator.nextInt(100)
      if (store.contains(key) && versionNum < store(key).versionNum) {
        println(s"client ${myNodeID} version check failed")
        sender() ! false
      } else if (rand > 101) {
        // write failed
        println(s"version check passed, but failed to write into store")
        sender ! false
      } else {
        // write success
        println(s"client ${myNodeID} write success")
        store.put(key, new StoredData(value, versionNum))
        sender() ! true
      }
    case Get(key) =>
      val rand = generator.nextInt(100)
      if (rand > 95 || !store.contains(key)) {
        sender ! new ReturnData(false, -1, -1)
      } else {
        sender ! new ReturnData(true, store(key).value, store(key).versionNum)
      }
  }
}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}
