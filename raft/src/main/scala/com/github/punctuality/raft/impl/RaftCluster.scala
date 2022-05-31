package com.github.punctuality.raft.impl

import cats.Monad
import cats.effect.{Async, Resource}
import com.github.punctuality.raft._
import com.github.punctuality.raft.model.{Configuration, LogCompactionPolicy}
import com.github.punctuality.raft.rpc.{RpcClientBuilder, RpcServerBuilder}
import com.github.punctuality.raft.storage.{StateMachine, Storage}
import com.github.punctuality.raft.util.Logger

object RaftCluster {

  def resource[F[_]: Async: RpcServerBuilder: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): Resource[F, Cluster[F]] =
    resource(
      config,
      storage,
      stateMachine,
      if (config.logCompactionThreshold <= 0)
        LogCompactionPolicy.noCompaction
      else
        LogCompactionPolicy.fixedSize(config.logCompactionThreshold)
    )

  def resource[F[_]: Async: RpcServerBuilder: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): Resource[F, Cluster[F]] =
    for {
      raft    <- Resource.eval(RaftImpl.build(config, storage, stateMachine, compactionPolicy))
      server  <- Resource.eval(RpcServerBuilder[F].build(config.local, raft))
      acquire  = Monad[F].pure(new Cluster[F](server, raft))
      cluster <- Resource.make(acquire)(_.stop)
    } yield cluster
}
