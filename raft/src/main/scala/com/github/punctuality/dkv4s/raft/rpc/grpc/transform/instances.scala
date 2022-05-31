package com.github.punctuality.dkv4s.raft.rpc.grpc.transform

import cats.effect.Sync
import cats.syntax.functor._
import com.github.punctuality.dkv4s.raft.model.{Command, LogEntry, Node, Snapshot}
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.ProtoSerializer
import io.scalaland.chimney._
import io.scalaland.chimney.dsl._
import com.google.protobuf.ByteString
import raft.rpc
import raft.rpc.InstallSnapshotRequest

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

  private val snapLimit: Int = 1 << 20
  type SnapshotStream[F[_]] = fs2.Stream[F, rpc.InstallSnapshotRequest]

  implicit def protoSnapshotReq2Domain[F[_]: Sync](implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration]
  ): Transformer[SnapshotStream[F], F[InstallSnapshot]] =
    (src: SnapshotStream[F]) =>
      src.compile
        .fold(List.empty[ByteString] -> Option.empty[InstallSnapshot]) {
          case (
                (buffers, None),
                rpc.InstallSnapshotRequest(-1, -1, None, snapshotChunk, ByteString.EMPTY, _)
              ) =>
            (buffers :+ snapshotChunk) -> None
          case (
                (buffers, None),
                rpc
                  .InstallSnapshotRequest(
                    raftId,
                    lastIndexId,
                    lastEntry,
                    ByteString.EMPTY,
                    config,
                    _
                  )
              ) =>
            buffers -> Some(
              InstallSnapshot(
                raftId,
                Snapshot(lastIndexId, null, configSer.decode(config)),
                lastEntry.transformInto[rpc.LogEntry].transformInto[LogEntry]
              )
            )
        }
        .map { case (buffers, Some(snapshotToFill)) =>
          val newBB = ByteBuffer.allocate(buffers.map(_.size).sum)
          buffers.foreach(binStr => newBB.put(binStr.asReadOnlyByteBuffer()))
          snapshotToFill.copy(snapshot = snapshotToFill.snapshot.copy(bytes = newBB))
        }

  implicit def snapshotReq2Proto[F[_]: Sync](implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration]
  ): Transformer[InstallSnapshot, SnapshotStream[F]] = {
    case InstallSnapshot(raftId, Snapshot(lastIndex, bytes, config), lastEntry) =>
      val dataStream =
        fs2.Stream
          .chunk(fs2.Chunk.ByteBuffer(bytes.asReadOnlyBuffer()))
          .chunkLimit(snapLimit)
          .map(byteChunk => ByteString.copyFrom(byteChunk.toByteBuffer))
          .map(rpc.InstallSnapshotRequest(-1, -1, None, _, ByteString.EMPTY))
      val configStream = fs2.Stream.eval(
        Sync[F].delay(
          rpc.InstallSnapshotRequest(
            raftId,
            lastIndex,
            Some(lastEntry.transformInto[rpc.LogEntry]),
            ByteString.EMPTY,
            configSer.encode(config)
          )
        )
      )
      dataStream ++ configStream
  }

  implicit val joinReq2Node: Transformer[rpc.JoinRequest, Node] = Transformer.derive
}
