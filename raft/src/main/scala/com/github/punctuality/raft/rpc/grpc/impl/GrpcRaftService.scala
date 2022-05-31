package com.github.punctuality.raft.rpc.grpc.impl

import cats.effect.Async
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.applicativeError._
import com.github.punctuality.raft.Raft
import com.github.punctuality.raft.model.{Command, LogEntry, Node, Snapshot}
import com.github.punctuality.raft.protocol._
import com.github.punctuality.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.raft.rpc.grpc.transform.instances._
import com.github.punctuality.raft.util.Logger
import io.grpc.Metadata
import io.scalaland.chimney.dsl._
import raft.rpc

class GrpcRaftService[F[_]: Async](raft: Raft[F])(implicit
  val logger: Logger[F],
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends rpc.RaftFs2Grpc[F, Metadata] {

  override def vote(request: rpc.VoteRequest, ctx: Metadata): F[rpc.VoteResponse] =
    raft
      .onReceive(request.transformInto[VoteRequest])
      .map(_.transformInto[rpc.VoteResponse])
      .onError { error =>
        logger.warn(s"Error during the VoteRequest process. Error ${error.getMessage}")
      }

  override def appendEntries(request: rpc.AppendEntriesRequest,
                             ctx: Metadata
  ): F[rpc.AppendEntriesResponse] =
    raft
      .onReceive(request.transformInto[AppendEntries])
      .map(_.transformInto[rpc.AppendEntriesResponse])

  override def execute(request: rpc.CommandRequest, ctx: Metadata): F[rpc.CommandResponse] =
    raft
      .onCommand(commandSer.decode(request.command))
      .map(response => rpc.CommandResponse(objectSer.encode(response)))
      .onError { error =>
        logger.warn(s"An error during the command process. Error ${error.getMessage}")
      }

  override def installSnapshot(request: rpc.InstallSnapshotRequest,
                               ctx: Metadata
  ): F[rpc.AppendEntriesResponse] =
    raft
      .onReceive(request.transformInto[InstallSnapshot])
      .map(_.transformInto[rpc.AppendEntriesResponse])
      .onError { error =>
        logger.warn(s"An error during snapshot installation. Error ${error.getMessage}")
      }

  override def join(request: rpc.JoinRequest, ctx: Metadata): F[rpc.JoinResponse] =
    raft.addMember(request.transformInto[Node]).as(rpc.JoinResponse())
}
