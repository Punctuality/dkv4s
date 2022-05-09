package com.github.punctuality.raft

import cats.Monad
import cats.syntax.flatMap._
import com.github.punctuality.raft.model.{Command, Node}
import com.github.punctuality.raft.rpc.RpcServer

class Cluster[F[_]: Monad](rpc: RpcServer[F], raft: Raft[F]) {

  def start: F[Node] =
    raft.initialize >> rpc.start >> raft.start

  def stop: F[Unit] =
    rpc.stop >> raft.stop

  def join(node: Node): F[Node] =
    raft.initialize >> rpc.start >> raft.join(node)

  def leave: F[Unit] =
    raft.leave

  def leader: F[Node] =
    raft.listen

  def execute[T](command: Command[T]): F[T] =
    raft.onCommand(command)
}
