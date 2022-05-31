package com.github.punctuality.dkv4s.raft.service

import com.github.punctuality.dkv4s.raft.model.Node
import com.github.punctuality.dkv4s.raft.protocol.ClusterConfiguration

import scala.collection.immutable.Set

/** Storage for current cluster configuration
  */
trait ClusterConfigStorage[F[_]] {

  /** @return Current nodes in the cluster
    */
  def members: F[Set[Node]]

  /** Applies new configuration to the cluster
    * @param newConfig New config to apply
    */
  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit]

  /** Get current cluster configuration
    * @return Current [[ClusterConfiguration]]
    */
  def getClusterConfiguration: F[ClusterConfiguration]
}
