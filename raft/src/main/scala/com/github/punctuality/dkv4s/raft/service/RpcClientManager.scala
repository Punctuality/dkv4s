package com.github.punctuality.dkv4s.raft.service

import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.protocol._

trait RpcClientManager[F[_]] {

  def sendVote(serverId: Node, voteRequest: VoteRequest): F[VoteResponse]

  def sendEntries(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse]

  def sendSnapshot(serverId: Node, snapshot: InstallSnapshot): F[AppendEntriesResponse]

  def sendCommand[T](serverId: Node, raftId: Int, command: Command[T]): F[T]

  def join(serverId: Node, raftId: Int, newNode: Node): F[Boolean]

  def closeConnections(): F[Unit]

}
