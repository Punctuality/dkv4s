package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.Node

case class VoteRequest(nodeId: Node, term: Long, lastLogIndex: Long, lastLogTerm: Long)
