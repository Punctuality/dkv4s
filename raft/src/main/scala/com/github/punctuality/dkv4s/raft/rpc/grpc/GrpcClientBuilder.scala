package com.github.punctuality.dkv4s.raft.rpc.grpc

import cats.effect.{Async, Resource}
import cats.syntax.functor._
import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.protocol.ClusterConfiguration
import com.github.punctuality.dkv4s.raft.rpc.grpc.impl.GrpcRaftClient
import com.github.punctuality.dkv4s.raft.rpc.{RpcClient, RpcClientBuilder}
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.dkv4s.raft.util.Logger
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import fs2.grpc.syntax.all._
import raft.rpc.RaftFs2Grpc

class GrpcClientBuilder[F[_]: Async: Logger](implicit
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends RpcClientBuilder[F] {

  override def build(address: Node): F[RpcClient[F]] =
    GrpcClientBuilder
      .mkStub(address.host, address.port)
      .allocated
      .map(new GrpcRaftClient[F](address, _))
}

object GrpcClientBuilder {
  def mkStub[F[_]: Async](host: String, port: Int): Resource[F, RaftFs2Grpc[F, Metadata]] =
    NettyChannelBuilder
      .forAddress(host, port)
      .disableRetry()
      .usePlaintext()
      .resource[F]
      .flatMap(RaftFs2Grpc.stubResource[F](_))

  def apply[F[_]: Async: Logger](implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration],
    objectSer: ProtoSerializer[Any]
  ): GrpcClientBuilder[F] = new GrpcClientBuilder
}
