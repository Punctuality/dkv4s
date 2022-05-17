package com.github.punctuality.dkv4s.raft.protocol

import com.github.punctuality.dkv4s.raft.model.{LogEntry, Snapshot}

case class InstallSnapshot(raftId: Int, snapshot: Snapshot, lastEntry: LogEntry)
