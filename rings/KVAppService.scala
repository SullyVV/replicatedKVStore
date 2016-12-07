package rings

import akka.actor.{ActorSystem, ActorRef, Props}

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class TestRead(key: BigInt) extends AppServiceAPI
case class TestWrite(key: BigInt, value: Int) extends AppServiceAPI

/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */


object KVAppService {

  def apply(system: ActorSystem, numClient: Int, numStore: Int, numReplica: Int, numRead: Int, numWrite: Int): ActorRef = {
    /** Storage tier: create K/V store servers */
    // lets assume there are 5 stores in our replicated server KVSttore
    val stores = for (i <- 0 until numStore)
      yield system.actorOf(KVStore.props(), "RingStore" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numClient)
      yield system.actorOf(RingServer.props(i, numClient, stores, numReplica, numRead, numWrite, numStore), "RingServer" + i)

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */


    /** Tells each server the list of servers and their ActorRefs wrapped in a message. */
    for (server <- servers)
      server ! View(servers)

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numClient, servers), "LoadMaster")
    master
  }
}

