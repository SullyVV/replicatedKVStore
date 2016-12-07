package rings

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(20 seconds)
  val numClient = 1
  val numStore = 1
  val numReplica = 3
  val numRead = 3
  val numWrite = 3
  private val storeTable = new scala.collection.mutable.HashMap[Int, Int]
  storeTable.put(36, 1)
  storeTable.put(72, 2)
  storeTable.put(108, 3)
  storeTable.put(144, 4)
  storeTable.put(180, 5)
  storeTable.put(216, 6)
  storeTable.put(248, 7)
  storeTable.put(280, 8)
  storeTable.put(312, 9)
  storeTable.put(360, 0)
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numClient, numStore, numReplica, numRead, numWrite,storeTable)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val future = ask(master, Start())
    Await.result(future, timeout.duration).asInstanceOf[Boolean]
    //Thread.sleep(1000)
    system.shutdown()
  }

}
