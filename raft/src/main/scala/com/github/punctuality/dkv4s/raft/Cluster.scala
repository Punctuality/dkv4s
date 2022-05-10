package com.github.punctuality.dkv4s.raft

import cats.Monad
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.rpc.RpcServer

class Cluster[F[_]: Monad](val rpc: RpcServer[F], val raft: Raft[F]) {

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
