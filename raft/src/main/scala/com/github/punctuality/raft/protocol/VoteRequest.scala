package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.Node

case class VoteRequest(nodeId: Node, term: Long, lastLogIndex: Long, lastLogTerm: Long)
