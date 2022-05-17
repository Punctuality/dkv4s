package com.github.punctuality.dkv4s.raft.model

import com.github.punctuality.dkv4s.raft.node.{FollowerNode, NodeState}

case class PersistedState(term: Long, votedFor: Option[Node], appliedIndex: Long = 0L) {
  def toNodeState(raftId: Int, nodeId: Node): NodeState =
    FollowerNode(raftId, nodeId, term, votedFor = votedFor)
}
