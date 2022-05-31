package com.github.punctuality.dkv4s.raft.rpc.grpc.impl

import cats.effect.{Async, Ref}
import cats.syntax.applicativeError._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.raft.Raft
import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.dkv4s.raft.rpc.grpc.transform.instances._
import com.github.punctuality.dkv4s.raft.storage.StateMachine
import com.github.punctuality.dkv4s.raft.util.Logger
import io.grpc.Metadata
import io.scalaland.chimney.dsl._
import raft.rpc

class GrpcRaftService[F[_]: Async](raftRef: Ref[F, Map[Int, Raft[F, StateMachine]]])(implicit
  val logger: Logger[F],
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends rpc.RaftFs2Grpc[F, Metadata] {

  private def useRaftById[T](raftId: Int)(op: Raft[F, StateMachine] => F[T]): F[T] =
    raftRef.get.flatMap(rafts =>
      rafts.get(raftId) match {
        case Some(raft) => op(raft)
        case None =>
          new RuntimeException(
            s"Failed to find raft for id: $raftId (has only ${rafts.keys.mkString(", ")})"
          ).raiseError
      }
    )
  override def vote(request: rpc.VoteRequest, ctx: Metadata): F[rpc.VoteResponse] =
    useRaftById(request.raftId)(
      _.onVote(request.transformInto[VoteRequest])
        .map(_.transformInto[rpc.VoteResponse])
        .onError { error =>
          logger.warn(s"Error during the VoteRequest process. Error ${error.getMessage}")
        }
    )

  override def appendEntries(request: rpc.AppendEntriesRequest,
                             ctx: Metadata
  ): F[rpc.AppendEntriesResponse] =
    useRaftById(request.raftId)(
      _.onAppendEntries(request.transformInto[AppendEntries])
        .map(_.transformInto[rpc.AppendEntriesResponse])
    )

  override def execute(request: rpc.CommandRequest, ctx: Metadata): F[rpc.CommandResponse] =
    useRaftById(request.raftId)(
      _.onCommand(commandSer.decode(request.command))
        .map(response => rpc.CommandResponse(request.raftId, objectSer.encode(response)))
        .onError { error =>
          logger.warn(s"An error during the command process. Error ${error.getMessage}")
        }
    )

  override def installSnapshot(request: SnapshotStream[F],
                               ctx: Metadata
  ): F[rpc.AppendEntriesResponse] =
    request
      .transformInto[F[InstallSnapshot]]
      .flatMap(request =>
        useRaftById(request.raftId)(
          _.onSnapshot(request)
            .map(_.transformInto[rpc.AppendEntriesResponse])
            .onError { error =>
              logger.warn(s"An error during snapshot installation. Error ${error.getMessage}")
            }
        )
      )

  override def join(request: rpc.JoinRequest, ctx: Metadata): F[rpc.JoinResponse] =
    useRaftById(request.raftId)(
      _.addMember(request.transformInto[Node]).as(rpc.JoinResponse(request.raftId))
    )
}
