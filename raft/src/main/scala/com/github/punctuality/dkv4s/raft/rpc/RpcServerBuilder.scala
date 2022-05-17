package com.github.punctuality.dkv4s.raft.rpc

import cats.effect.Ref
import com.github.punctuality.dkv4s.raft.Raft
import com.github.punctuality.dkv4s.raft.model.Node
import com.github.punctuality.dkv4s.raft.storage.StateMachine
import com.github.punctuality.dkv4s.raft.util.Logger

trait RpcServerBuilder[F[_]] {
  def build(node: Node, rafts: RpcServerBuilder.RaftMap[F])(implicit L: Logger[F]): F[RpcServer[F]]
}

object RpcServerBuilder {
  type RaftMap[F[_]] = Ref[F, Map[Int, Raft[F, StateMachine]]]

  def apply[F[_]](implicit builder: RpcServerBuilder[F]): RpcServerBuilder[F] = builder
}
