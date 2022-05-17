package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.Node

sealed trait ClusterConfiguration {
  def members: Set[Node]
  def quorumReached(nodes: Set[Node]): Boolean
}
// TODO Add new quorum policy
case class NewClusterConfiguration(members: Set[Node]) extends ClusterConfiguration {

  private val quorum = (members.size / 2) + 1

  override def quorumReached(nodes: Set[Node]): Boolean =
    nodes.intersect(members).size >= quorum
}

case class JointClusterConfiguration(oldMembers: Set[Node], newMembers: Set[Node])
  extends ClusterConfiguration {

  private val oldQuorum = (oldMembers.size / 2) + 1
  private val newQuorum = (newMembers.size / 2) + 1

  override def members: Set[Node] =
    oldMembers ++ newMembers

  override def quorumReached(nodes: Set[Node]): Boolean =
    (nodes.intersect(oldMembers).size >= oldQuorum) &&
      (nodes.intersect(newMembers).size >= newQuorum)
}
