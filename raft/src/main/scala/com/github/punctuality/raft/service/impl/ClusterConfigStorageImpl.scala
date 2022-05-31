package com.github.punctuality.raft.service.impl

import cats.Monad
import cats.syntax.functor._
import cats.effect.{Ref, Sync}
import com.github.punctuality.raft.service.ClusterConfigStorage
import com.github.punctuality.raft.model.Node
import com.github.punctuality.raft.protocol.{ClusterConfiguration, NewClusterConfiguration}

class ClusterConfigStorageImpl[F[_]: Monad](configurationRef: Ref[F, ClusterConfiguration])
  extends ClusterConfigStorage[F] {

  def members: F[Set[Node]] =
    configurationRef.get.map(_.members)

  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] =
    configurationRef.set(newConfig)

  def getClusterConfiguration: F[ClusterConfiguration] =
    configurationRef.get

}

object ClusterConfigStorageImpl {
  def build[F[_]: Monad: Sync](members: Set[Node]): F[ClusterConfigStorageImpl[F]] =
    Ref
      .of[F, ClusterConfiguration](NewClusterConfiguration(members))
      .map(new ClusterConfigStorageImpl[F](_))
}
