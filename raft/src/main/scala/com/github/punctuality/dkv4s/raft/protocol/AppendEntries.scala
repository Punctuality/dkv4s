package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.{LogEntry, Node}

case class AppendEntries(raftId: Int,
                         leaderId: Node,
                         term: Long,
                         prevLogIndex: Long,
                         prevLogTerm: Long,
                         leaderCommit: Long,
                         entries: List[LogEntry]
)
