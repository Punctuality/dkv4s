package com.github.punctuality.dkv4s.raft.rpc

import com.github.punctuality.dkv4s.raft.model.{Command, LogEntry, Node, Snapshot}
import com.github.punctuality.dkv4s.raft.protocol.{AppendEntries, AppendEntriesResponse, InstallSnapshot, VoteRequest, VoteResponse}
import com.github.punctuality.raft.protocol._

trait RpcClient[F[_]] {
  def send(voteRequest: VoteRequest): F[VoteResponse]

  def send(appendEntries: AppendEntries): F[AppendEntriesResponse]

  def send[T](command: Command[T]): F[T]

  def send(snapshot: InstallSnapshot): F[AppendEntriesResponse]

  def join(server: Node): F[Boolean]

  def close(): F[Unit]
}
