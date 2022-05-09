package com.github.punctuality.raft.rpc

import com.github.punctuality.raft.model.Node

trait RpcClientBuilder[F[_]] {
  def build(address: Node): F[RpcClient[F]]
}

object RpcClientBuilder {
  def apply[F[_]](implicit ev: RpcClientBuilder[F]): RpcClientBuilder[F] = ev
}
