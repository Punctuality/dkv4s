package test

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.github.punctuality.dkv4s.cluster.util.Emojis
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

  private val rnd                           = new Random()
  private def pickRandom[A](seq: Seq[A]): A = seq(rnd.between(0, seq.size))
  private def rndByteStr(len: Int): String  = new String(rnd.nextBytes(len))

  implicit val commandSer: ProtoSerializer[Command[_]]          = JavaProtoSerializer.anySerObject
  implicit val configSer: ProtoSerializer[ClusterConfiguration] = JavaProtoSerializer.anySerObject
  implicit val objectSer: ProtoSerializer[Any]                  = JavaProtoSerializer.anySerObject

  private val nodeCount = 3
  private val nodes     = (1 to nodeCount).toList.map(i => Node("localhost", 8880 + i))
  private val configs =
    nodes.map(node =>
      Configuration(
        local                  = node,
        members                = nodes.filter(_ != node),
        electionMinDelayMillis = 100,
        electionMaxDelayMillis = 600,
        logCompactionThreshold = 10_000
      )
    )

  private val batchLimit   = 10_000
  private val entriesCount = 1_000_000
  private val entries      = fs2.Stream.range(1, entriesCount + 1).map(i => s"key$i" -> rndByteStr(5))

  override def run(args: List[String]): IO[ExitCode] =
    configs.zipWithIndex
      .foldLeft(Resource.eval(List.empty[Cluster[IO, KvStateMachine]].pure[IO])) {
        case (clusters, (config, index)) =>
          for {
            acc <- clusters
            curCluster <-
              makeCluster(0, s"${Emojis.randomEmoji} Cluster #${index + 1}", config)
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
          startTime <- IO.realTime
          _ <- entries
                 .chunkN(batchLimit)
                 .lift[IO]
                 .parEvalMap(2)(chunk =>
                   pickRandom(clusters).execute(SetManyCommand(chunk.toList)).as(chunk.size)
                 )
                 .scan(0L)(_ + _)
                 .debug(total => s"Processed entries chunk of size $batchLimit (total: $total)")
                 .compile
                 .drain
          endTime <- IO.realTime
          _       <- IO(println(s"💥💥💥 Set commands were executed ${endTime - startTime}"))
          _       <- IO.sleep((configs.head.heartbeatIntervalMillis * 1.5).millis)
          _       <- IO(println("💥💥💥 Retrieving results..."))
          allResults <-
            (entries.take(5).toList ++ entries.drop(entriesCount - 5).toList).traverse {
              case (key, _) =>
                clusters.traverse(_.execute(GetCommand(key)))
            }
          _ <- IO(println(s"💥💥💥 Result of the get commands are:\n${allResults.mkString(",\n")}"))
        } yield ExitCode.Success
      )

  //noinspection SameParameterValue
  private def makeCluster(raftId: Int,
                          name: String,
                          config: Configuration
  ): Resource[IO, Cluster[IO, KvStateMachine]] = {
    implicit val logger: Logger[IO] = ConsoleLogger[IO](name, ConsoleLogger.Level.Info)

    implicit val clientBuilder: GrpcClientBuilder[IO] = GrpcClientBuilder[IO]
    implicit val serverBuilder: GrpcServerBuilder[IO] = GrpcServerBuilder[IO]

    for {
      stateMachine <- Resource.eval(KvStateMachine.empty[IO])
      storage      <- Resource.eval(MemoryStorage.empty[IO])
      cluster      <- RaftCluster.resource(raftId, config, storage, stateMachine)
    } yield cluster
  }
}
