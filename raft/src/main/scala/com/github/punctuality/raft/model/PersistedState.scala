package com.github.punctuality.raft.model

import com.github.punctuality.raft.node.{FollowerNode, NodeState}

case class PersistedState(term: Long, votedFor: Option[Node], appliedIndex: Long = 0L) {
  def toNodeState(nodeId: Node): NodeState =
    FollowerNode(nodeId, term, votedFor = votedFor)
}
