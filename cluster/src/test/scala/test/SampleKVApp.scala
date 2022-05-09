package test

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.github.punctuality.raft.Cluster
import com.github.punctuality.raft.impl.RaftCluster
import com.github.punctuality.raft.model._
import com.github.punctuality.raft.protocol.ClusterConfiguration
import com.github.punctuality.raft.rpc.grpc.serializer.{JavaProtoSerializer, ProtoSerializer}
import com.github.punctuality.raft.rpc.grpc.{GrpcClientBuilder, GrpcServerBuilder}
import com.github.punctuality.raft.storage.impl.memory.MemoryStorage
import com.github.punctuality.raft.util.Logger
import com.github.punctuality.raft.util.console.ConsoleLogger

object SampleKVApp extends IOApp {

  private val config = Configuration(Node("localhost", 8090), List.empty)

  implicit val logger: Logger[IO]                               = ConsoleLogger[IO](ConsoleLogger.Level.Trace)
  implicit val commandSer: ProtoSerializer[Command[_]]          = JavaProtoSerializer.anySerObject
  implicit val configSer: ProtoSerializer[ClusterConfiguration] = JavaProtoSerializer.anySerObject
  implicit val objectSer: ProtoSerializer[Any]                  = JavaProtoSerializer.anySerObject

  implicit val clientBuilder: GrpcClientBuilder[IO] = GrpcClientBuilder[IO]
  implicit val serverBuilder: GrpcServerBuilder[IO] = GrpcServerBuilder[IO]

  override def run(args: List[String]): IO[ExitCode] =
    makeCluster(config).use { cluster =>
      for {
        leader <- cluster.start
        _      <- IO(println(s"Election is completed. Leader is $leader"))
        _      <- cluster.execute(SetCommand("key", "value"))
        _      <- IO(println("Set command is executed"))
        result <- cluster.execute(GetCommand("key"))
        _      <- IO(println(s"Result of the get command is : $result"))
      } yield ExitCode.Success
    }

  private def makeCluster(config: Configuration): Resource[IO, Cluster[IO]] =
    for {
      stateMachine <- Resource.eval(KvStateMachine.empty)
      storage      <- Resource.eval(MemoryStorage.empty[IO])
      cluster      <- RaftCluster.resource(config, storage, stateMachine)
    } yield cluster
}
