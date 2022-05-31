package com.github.punctuality.dkv4s.raft.rpc

import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.protocol._

trait RpcClient[F[_]] {
  def sendVote(voteRequest: VoteRequest): F[VoteResponse]

  def sendEntries(appendEntries: AppendEntries): F[AppendEntriesResponse]

  def sendCommand[T](raftId: Int, command: Command[T]): F[T]

  def sendSnapshot(snapshot: InstallSnapshot): F[AppendEntriesResponse]

  def join(raftId: Int, server: Node): F[Boolean]

  def close(): F[Unit]
}
