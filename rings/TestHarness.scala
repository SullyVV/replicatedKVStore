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
  val numClient = 2
  val numStore = 10
  val numReplica = 3
  val numRead = 3
  val numWrite = 3
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numClient, numStore, numReplica, numRead, numWrite)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val future = ask(master, Start())
    Await.result(future, timeout.duration).asInstanceOf[Boolean]
    //Thread.sleep(1000)
    system.shutdown()
  }

}
