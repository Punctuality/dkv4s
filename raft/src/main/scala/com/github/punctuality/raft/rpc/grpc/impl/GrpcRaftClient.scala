package com.github.punctuality.raft.rpc.grpc.impl

import cats.MonadThrow
import cats.syntax.functor._
import cats.syntax.applicativeError._
import com.github.punctuality.raft.model.{Command, Node}
import com.github.punctuality.raft.protocol._
import com.github.punctuality.raft.rpc.grpc.transform.instances._
import com.github.punctuality.raft.rpc.RpcClient
import com.github.punctuality.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.raft.util.Logger
import io.grpc.Metadata
import io.scalaland.chimney.dsl._
import raft.rpc
import raft.rpc.RaftFs2Grpc

class GrpcRaftClient[F[_]: MonadThrow](address: Node, stub: (RaftFs2Grpc[F, Metadata], F[Unit]))(
  implicit
  logger: Logger[F],
  commandSer: ProtoSerializer[Command[_]],
  configSer: ProtoSerializer[ClusterConfiguration],
  objectSer: ProtoSerializer[Any]
) extends RpcClient[F] {

  private val stubService: RaftFs2Grpc[F, Metadata] = stub._1
  private val emptyMetadata: Metadata               = new Metadata()

  override def send(voteRequest: VoteRequest): F[VoteResponse] =
    stubService
      .vote(voteRequest.transformInto[rpc.VoteRequest], emptyMetadata)
      .map(_.transformInto[VoteResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending VoteRequest to node: $address, Error: ${error.getMessage}"
        )
      }

  override def send(appendEntries: AppendEntries): F[AppendEntriesResponse] =
    stubService
      .appendEntries(appendEntries.transformInto[rpc.AppendEntriesRequest], emptyMetadata)
      .map(_.transformInto[AppendEntriesResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending AppendEntries request to node: $address, Error: ${error.getMessage}"
        )
      }

  override def send[T](command: Command[T]): F[T] =
    stubService
      .execute(rpc.CommandRequest(commandSer.encode(command)), emptyMetadata)
      .map(resp => objectSer.decode(resp.output).asInstanceOf[T])
      .onError { error =>
        logger.warn(
          s"An error in sending a command to node: $address. Command: $command, Error: ${error.getMessage}"
        )
      }

  override def send(snapshot: InstallSnapshot): F[AppendEntriesResponse] =
    stubService
      .installSnapshot(snapshot.transformInto[rpc.InstallSnapshotRequest], emptyMetadata)
      .map(_.transformInto[AppendEntriesResponse])
      .onError { error =>
        logger.warn(
          s"An error in sending a snapshot to node: $address. Snapshot: $snapshot, Error: ${error.getMessage}"
        )
      }

  override def join(server: Node): F[Boolean] =
    stubService
      .join(server.transformInto[rpc.JoinRequest], emptyMetadata)
      .as(true)

  override def close(): F[Unit] = stub._2
}
