package rings

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(20 seconds)
  val numClient = 3
  val numStore = 10
  val numReplica = 3  // N
  val numRead = 2   // R
  val numWrite = 2    // W
  private val storeTable = new scala.collection.mutable.HashMap[Int, Int]
  storeTable.put(36, 1)
  storeTable.put(72, 2)
  storeTable.put(108, 3)
  storeTable.put(144, 4)
  storeTable.put(180, 5)
  storeTable.put(216, 6)
  storeTable.put(252, 7)
  storeTable.put(288, 8)
  storeTable.put(324, 9)
  storeTable.put(360, 0)
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numClient, numStore, numReplica, numRead, numWrite,storeTable)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val future = ask(master, Start())
    val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
    val future2 = ask(master, Report())
    val done2 = Await.result(future2, timeout.duration).asInstanceOf[mutable.HashMap[BigInt, Stat]]
    println()
    println(s"correctness check result:")
    println()
    for ((k,v) <- done2) {
      println(s"key ${k} has success write: ${v.successWrite} and fail write: ${v.failWrite}")
    }
    Thread.sleep(30000)
    system.shutdown()
  }

}
