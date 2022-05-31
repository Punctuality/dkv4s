package com.github.punctuality.raft.rpc.grpc

import cats.effect.{Async, Sync}
import cats.syntax.functor._
import com.github.punctuality.raft.Raft
import com.github.punctuality.raft.model.{Command, Node}
import com.github.punctuality.raft.protocol.ClusterConfiguration
import com.github.punctuality.raft.rpc.grpc.impl.GrpcRaftService
import com.github.punctuality.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.raft.rpc.{RpcServer, RpcServerBuilder}
import com.github.punctuality.raft.util.Logger
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import fs2.grpc.syntax.all._
import io.grpc._
import raft.rpc._

class GrpcServerBuilder[F[_]: Async: Logger](implicit
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends RpcServerBuilder[F] {
  override def build(node: Node, raft: Raft[F]): F[RpcServer[F]] =
    GrpcServerBuilder.construct(new GrpcRaftService[F](raft), node.port)
}

object GrpcServerBuilder {
  def construct[F[_]: Async](impl: RaftFs2Grpc[F, Metadata], port: Int): F[RpcServer[F]] =
    RaftFs2Grpc
      .bindServiceResource[F](impl)
      .flatMap(service => NettyServerBuilder.forPort(port).addService(service).resource[F])
      .allocated
      .map { case (serverInst, release) =>
        new RpcServer[F] {
          override val server: Server = serverInst
          override def start: F[Unit] = Async[F].delay(server.start)
          override def stop: F[Unit]  = release
        }
      }

  def apply[F[_]: Async: Logger](implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration],
    objectSer: ProtoSerializer[Any]
  ): GrpcServerBuilder[F] = new GrpcServerBuilder
}
