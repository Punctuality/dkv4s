package com.github.punctuality.raft.rpc.grpc.transform

import com.github.punctuality.raft.model.{Command, LogEntry, Node, Snapshot}
import io.scalaland.chimney._
import io.scalaland.chimney.dsl._
import com.github.punctuality.raft.protocol._
import com.github.punctuality.raft.rpc.grpc.serializer.ProtoSerializer
import com.google.protobuf.ByteString
import raft.rpc

import java.nio.ByteBuffer

object instances {
  implicit def id2Node: Transformer[String, Node] = (str: String) => Node.fromString(str).get
  implicit def node2Id: Transformer[Node, String] = (node: Node) => node.id

  implicit def val2Option[T]: Transformer[T, Option[T]] = (src: T) => Some(src)
  implicit def option2Val[T]: Transformer[Option[T], T] = (src: Option[T]) => src.get

  implicit def protoLogEntry2Domain(implicit
    commandSer: ProtoSerializer[Command[_]]
  ): Transformer[rpc.LogEntry, LogEntry] =
    Transformer
      .define[rpc.LogEntry, LogEntry]
      .withFieldComputed(_.command, e => commandSer.decode(e.command))
      .buildTransformer

  implicit def logEntry2Proto(implicit
    commandSer: ProtoSerializer[Command[_]]
  ): Transformer[LogEntry, rpc.LogEntry] =
    Transformer
      .define[LogEntry, rpc.LogEntry]
      .withFieldComputed(_.command, e => commandSer.encode(e.command))
      .buildTransformer

  implicit val protoVoteReq2Domain: Transformer[rpc.VoteRequest, VoteRequest] =
    Transformer
      .define[rpc.VoteRequest, VoteRequest]
      .withFieldRenamed(_.currentTerm, _.term)
      .withFieldRenamed(_.logLength, _.lastLogIndex)
      .withFieldRenamed(_.logTerm, _.lastLogTerm)
      .buildTransformer

  implicit val voteReq2Proto: Transformer[VoteRequest, rpc.VoteRequest] =
    Transformer
      .define[VoteRequest, rpc.VoteRequest]
      .withFieldComputed(_.nodeId, _.nodeId.id)
      .withFieldRenamed(_.term, _.currentTerm)
      .withFieldRenamed(_.lastLogIndex, _.logLength)
      .withFieldRenamed(_.lastLogTerm, _.logTerm)
      .buildTransformer

  implicit val protoVoteResp2Domain: Transformer[rpc.VoteResponse, VoteResponse] =
    Transformer
      .define[rpc.VoteResponse, VoteResponse]
      .withFieldRenamed(_.granted, _.voteGranted)
      .buildTransformer

  implicit val voteResp2Proto: Transformer[VoteResponse, rpc.VoteResponse] =
    Transformer
      .define[VoteResponse, rpc.VoteResponse]
      .withFieldComputed(_.nodeId, _.nodeId.id)
      .withFieldRenamed(_.voteGranted, _.granted)
      .buildTransformer

  implicit def protoAppendReq2Domain(implicit
    commandSer: ProtoSerializer[Command[_]]
  ): Transformer[rpc.AppendEntriesRequest, AppendEntries] =
    Transformer
      .define[rpc.AppendEntriesRequest, AppendEntries]
      .withFieldRenamed(_.logLength, _.prevLogIndex)
      .withFieldRenamed(_.logTerm, _.prevLogTerm)
      .buildTransformer

  implicit def appendReq2Proto(implicit
    commandSer: ProtoSerializer[Command[_]]
  ): Transformer[AppendEntries, rpc.AppendEntriesRequest] =
    Transformer
      .define[AppendEntries, rpc.AppendEntriesRequest]
      .withFieldComputed(_.leaderId, _.leaderId.id)
      .withFieldRenamed(_.prevLogIndex, _.logLength)
      .withFieldRenamed(_.prevLogTerm, _.logTerm)
      .buildTransformer

  implicit val protoAppendResp2Domain: Transformer[rpc.AppendEntriesResponse, AppendEntriesResponse] =
    Transformer.derive

  implicit val appendResp2Proto: Transformer[AppendEntriesResponse, rpc.AppendEntriesResponse] =
    Transformer
      .define[AppendEntriesResponse, rpc.AppendEntriesResponse]
      .withFieldComputed(_.nodeId, _.nodeId.id)
      .buildTransformer

  implicit def protoSnapshotReq2Domain(implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration]
  ): Transformer[rpc.InstallSnapshotRequest, InstallSnapshot] =
    Transformer
      .define[rpc.InstallSnapshotRequest, InstallSnapshot]
      .withFieldComputed(
        _.snapshot,
        { case rpc.InstallSnapshotRequest(lastIndexId, _, bytes, config, _) =>
          Snapshot(lastIndexId, ByteBuffer.wrap(bytes.toByteArray), configSer.decode(config))
        }
      )
      .withFieldComputed(
        _.lastEntry,
        _.lastEntry.transformInto[rpc.LogEntry].transformInto[LogEntry]
      )
      .buildTransformer

  implicit def snapshotReq2Proto(implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration]
  ): Transformer[InstallSnapshot, rpc.InstallSnapshotRequest] =
    Transformer
      .define[InstallSnapshot, rpc.InstallSnapshotRequest]
      .withFieldComputed(_.lastIndexId, _.snapshot.lastIndex)
      .withFieldComputed(_.bytes, req => ByteString.copyFrom(req.snapshot.bytes))
      .withFieldComputed(_.config, req => configSer.encode(req.snapshot.config))
      .buildTransformer

  implicit val joinReq2Node: Transformer[rpc.JoinRequest, Node] = Transformer.derive
  implicit val node2JoinReq: Transformer[Node, rpc.JoinRequest] = Transformer.derive
}
