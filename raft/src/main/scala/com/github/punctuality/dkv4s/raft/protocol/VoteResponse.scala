package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.Node

case class VoteResponse(raftId: Int, nodeId: Node, term: Long, voteGranted: Boolean)
