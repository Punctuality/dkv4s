package com.github.punctuality.dkv4s.raft.service

import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.protocol._

trait RpcClientManager[F[_]] {

  def send(serverId: Node, voteRequest: VoteRequest): F[VoteResponse]

  def send(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse]

  def send(serverId: Node, snapshot: InstallSnapshot): F[AppendEntriesResponse]

  def send[T](serverId: Node, command: Command[T]): F[T]

  def join(serverId: Node, newNode: Node): F[Boolean]

  def closeConnections(): F[Unit]

}
