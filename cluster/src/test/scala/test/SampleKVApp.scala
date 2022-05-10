package test

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.github.punctuality.dkv4s.raft.Cluster
import com.github.punctuality.dkv4s.raft.impl.RaftCluster
import com.github.punctuality.dkv4s.raft.model.{Command, Configuration, Node}
import com.github.punctuality.dkv4s.raft.protocol.ClusterConfiguration
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.{JavaProtoSerializer, ProtoSerializer}
import com.github.punctuality.dkv4s.raft.rpc.grpc.{GrpcClientBuilder, GrpcServerBuilder}
import com.github.punctuality.dkv4s.raft.storage.impl.memory.MemoryStorage
import com.github.punctuality.dkv4s.raft.util.Logger
import com.github.punctuality.dkv4s.raft.util.console.ConsoleLogger

import scala.concurrent.duration._
import scala.util.Random

object SampleKVApp extends IOApp {

  private def pickRandom[A](seq: Seq[A]): A = seq(new Random().between(0, seq.size))

  private val nodeCount = 9
  private val nodes     = (1 to nodeCount).toList.map(i => Node("localhost", 8880 + i))
  private val configs =
    nodes.map(node =>
      Configuration(
        local                  = node,
        members                = nodes.filter(_ != node),
        electionMinDelayMillis = 100,
        electionMaxDelayMillis = 600
      )
    )

  private val entriesCount = 1000
  private val entries      = (1 to entriesCount).toList.map(i => s"key$i" -> s"value$i")

  override def run(args: List[String]): IO[ExitCode] =
    configs.zipWithIndex
      .foldLeft(Resource.eval(List.empty[Cluster[IO]].pure[IO])) {
        case (clusters, (config, index)) =>
          for {
            acc        <- clusters
            curCluster <- makeCluster(s"${Emojis.randomEmoji} Cluster #${index + 1}", config)
          } yield curCluster :: acc
      }
      .map(_.reverse)
      .use(clusters =>
        for {
          leaders <- IO.parTraverseN(nodeCount)(clusters)(_.start)
          _ <-
            IO(
              println(
                s"💥💥💥 ️Election is completed. Leader is ${leaders.head} (unanimously: ${leaders
                  .forall(_ == leaders.head)})"
              )
            )
          _ <- entries.traverse { case (key, value) =>
                 pickRandom(clusters).execute(SetCommand(key, value))
               }
          _ <- IO(println("💥💥💥 Set commands were executed"))
          _ <- IO.sleep((configs.head.heartbeatIntervalMillis * 1.5).millis)
          _ <- IO(println("💥💥💥 Retrieving results..."))
          allResults <-
            (entries.take(5) ++ entries.takeRight(5)).traverse { case (key, _) =>
              clusters.traverse(_.execute(GetCommand(key)))
            }
          _ <- IO(println(s"💥💥💥 Result of the get commands are:\n${allResults.mkString(",\n")}"))
        } yield ExitCode.Success
      )

  //noinspection SameParameterValue
  private def makeCluster(name: String, config: Configuration): Resource[IO, Cluster[IO]] = {
    implicit val logger: Logger[IO]                               = ConsoleLogger[IO](name, ConsoleLogger.Level.Debug)
    implicit val commandSer: ProtoSerializer[Command[_]]          = JavaProtoSerializer.anySerObject
    implicit val configSer: ProtoSerializer[ClusterConfiguration] = JavaProtoSerializer.anySerObject
    implicit val objectSer: ProtoSerializer[Any]                  = JavaProtoSerializer.anySerObject

    implicit val clientBuilder: GrpcClientBuilder[IO] = GrpcClientBuilder[IO]
    implicit val serverBuilder: GrpcServerBuilder[IO] = GrpcServerBuilder[IO]

    for {
      stateMachine <- Resource.eval(KvStateMachine.empty)
      storage      <- Resource.eval(MemoryStorage.empty[IO])
      cluster      <- RaftCluster.resource(config, storage, stateMachine)
    } yield cluster
  }
}
