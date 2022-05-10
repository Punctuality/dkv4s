package com.github.punctuality.dkv4s.raft.model

import com.github.punctuality.dkv4s.raft.node.{FollowerNode, NodeState}

case class PersistedState(term: Long, votedFor: Option[Node], appliedIndex: Long = 0L) {
  def toNodeState(nodeId: Node): NodeState =
    FollowerNode(nodeId, term, votedFor = votedFor)
}
