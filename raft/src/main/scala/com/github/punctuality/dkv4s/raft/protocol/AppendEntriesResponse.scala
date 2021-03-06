package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.Node

case class AppendEntriesResponse(raftId: Int,
                                 nodeId: Node,
                                 currentTerm: Long,
                                 ack: Long,
                                 success: Boolean
)
