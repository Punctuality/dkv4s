package com.github.punctuality.dkv4s.raft.impl

import cats.effect.{Async, Ref, Resource}
import com.github.punctuality.dkv4s.raft.{Cluster, Raft}
import com.github.punctuality.dkv4s.raft.model.{Configuration, LogCompactionPolicy}
import com.github.punctuality.dkv4s.raft.rpc.{RpcClientBuilder, RpcServerBuilder}
import com.github.punctuality.dkv4s.raft.storage.{StateMachine, Storage}
import com.github.punctuality.dkv4s.raft.util.Logger

object RaftCluster {

  def resource[F[_]: Async: RpcServerBuilder: RpcClientBuilder: Logger, SM[X[_]] <: StateMachine[
    X
  ]](raftId: Int,
     config: Configuration,
     storage: Storage[F],
     stateMachine: SM[F]
  ): Resource[F, Cluster[F, SM]] =
    resource(
      raftId,
      config,
      storage,
      stateMachine,
      if (config.logCompactionThreshold <= 0)
        LogCompactionPolicy.noCompaction
      else
        LogCompactionPolicy.fixedSize(config.logCompactionThreshold)
    )

  def resource[F[_]: Async: RpcServerBuilder: RpcClientBuilder: Logger, SM[X[_]] <: StateMachine[
    X
  ]](raftId: Int,
     config: Configuration,
     storage: Storage[F],
     stateMachine: SM[F],
     compactionPolicy: LogCompactionPolicy[F]
  ): Resource[F, Cluster[F, SM]] =
    for {
      raft                                    <- Resource.eval(RaftImpl.build(raftId, config, storage, stateMachine, compactionPolicy))
      initMap: Map[Int, Raft[F, StateMachine]] = Map(raftId -> raft)
      rafts                                   <- Resource.eval(Ref.of(initMap))
      server                                  <- Resource.eval(RpcServerBuilder[F].build(config.local, rafts))
      acquire                                  = Async[F].delay(new SingleRaftCluster[F, SM](server, raft))
      cluster                                 <- Resource.make(acquire)(_.stop)
    } yield cluster
}
