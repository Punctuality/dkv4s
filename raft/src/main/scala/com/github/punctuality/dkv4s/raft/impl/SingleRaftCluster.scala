package com.github.punctuality.dkv4s.raft.impl

import cats.Monad
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.raft.{Cluster, Raft}
import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.rpc.RpcServer
import com.github.punctuality.dkv4s.raft.storage.StateMachine

class SingleRaftCluster[F[_]: Monad, +SM[X[_]] <: StateMachine[X]](val rpc: RpcServer[F],
                                                                   val raft: Raft[F, SM]
) extends Cluster[F, SM] {

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
