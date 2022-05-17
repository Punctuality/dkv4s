package com.github.punctuality.dkv4s.raft.rpc.grpc.impl

import cats.effect.Sync
import cats.syntax.functor._
import cats.syntax.applicativeError._
import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.rpc.RpcClient
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.dkv4s.raft.rpc.grpc.transform.instances._
import com.github.punctuality.dkv4s.raft.util.Logger
import io.grpc.Metadata
import io.scalaland.chimney.dsl._
import raft.rpc
import raft.rpc.RaftFs2Grpc

class GrpcRaftClient[F[_]: Sync](address: Node, stub: (RaftFs2Grpc[F, Metadata], F[Unit]))(implicit
  logger: Logger[F],
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends RpcClient[F] {

  private val stubService: RaftFs2Grpc[F, Metadata] = stub._1
  private def emptyMetadata: Metadata               = new Metadata()

  override def sendVote(voteRequest: VoteRequest): F[VoteResponse] =
    stubService
      .vote(voteRequest.transformInto[rpc.VoteRequest], emptyMetadata)
      .map(_.transformInto[VoteResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending VoteRequest to node: $address, Error: ${error.getMessage}"
        )
      }

  override def sendEntries(appendEntries: AppendEntries): F[AppendEntriesResponse] =
    stubService
      .appendEntries(appendEntries.transformInto[rpc.AppendEntriesRequest], emptyMetadata)
      .map(_.transformInto[AppendEntriesResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending AppendEntries request to node: $address, Error: ${error.getMessage}"
        )
      }

  override def sendCommand[T](raftId: Int, command: Command[T]): F[T] =
    stubService
      .execute(rpc.CommandRequest(raftId, commandSer.encode(command)), emptyMetadata)
      .map(resp => objectSer.decode(resp.output).asInstanceOf[T])
      .onError { error =>
        logger.warn(
          s"An error in sending a command to node: $address. Command: $command, Error: ${error.getMessage}"
        )
      }

  override def sendSnapshot(snapshot: InstallSnapshot): F[AppendEntriesResponse] =
    stubService
      .installSnapshot(snapshot.transformInto[SnapshotStream[F]], emptyMetadata)
      .map(_.transformInto[AppendEntriesResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending a snapshot to node: $address. Snapshot: $snapshot, Error: ${error.getMessage}"
        )
      }

  override def join(raftId: Int, server: Node): F[Boolean] =
    stubService
      .join(rpc.JoinRequest(raftId, server.host, server.port), emptyMetadata)
      .as(true)

  override def close(): F[Unit] = stub._2
}
