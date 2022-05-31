package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.{LogEntry, Node}

case class AppendEntries(leaderId: Node,
                         term: Long,
                         prevLogIndex: Long,
                         prevLogTerm: Long,
                         leaderCommit: Long,
                         entries: List[LogEntry]
)
