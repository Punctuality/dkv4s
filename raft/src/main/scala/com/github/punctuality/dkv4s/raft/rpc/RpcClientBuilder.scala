package com.github.punctuality.dkv4s.raft.rpc

import com.github.punctuality.dkv4s.raft.model.Node

trait RpcClientBuilder[F[_]] {
  def build(address: Node): F[RpcClient[F]]
}

object RpcClientBuilder {
  def apply[F[_]](implicit ev: RpcClientBuilder[F]): RpcClientBuilder[F] = ev
}
