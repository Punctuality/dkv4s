package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.Node

case class VoteResponse(nodeId: Node, term: Long, voteGranted: Boolean)
