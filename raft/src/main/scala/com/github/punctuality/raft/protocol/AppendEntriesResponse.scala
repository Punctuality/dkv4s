package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.Node

case class AppendEntriesResponse(nodeId: Node, currentTerm: Long, ack: Long, success: Boolean)
