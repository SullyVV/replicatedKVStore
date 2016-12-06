package rings

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(60 seconds)
  val numClient = 3
  val numStore = 10
  val burstSize = 1
  val numReplica = 5
  val numRead = 3
  val numWrite = 3
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numClient, numStore, numReplica, numRead, numWrite, burstSize)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val future = ask(master, Start())
    Await.result(future, timeout.duration).asInstanceOf[Boolean]
    system.shutdown()
  }

}
