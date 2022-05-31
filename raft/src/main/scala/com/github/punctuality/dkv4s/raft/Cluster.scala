package com.github.punctuality.dkv4s.raft

import com.github.punctuality.dkv4s.raft.model.{Command, Node}
import com.github.punctuality.dkv4s.raft.rpc.RpcServer
import com.github.punctuality.dkv4s.raft.storage.StateMachine

trait Cluster[F[_], +SM[X[_]] <: StateMachine[X]] {
  val rpc: RpcServer[F]
  val raft: Raft[F, SM]

  def start: F[Node]

  def stop: F[Unit]

  def join(node: Node): F[Node]

  def leave: F[Unit]

  def leader: F[Node]

  def execute[T](command: Command[T]): F[T]
}
