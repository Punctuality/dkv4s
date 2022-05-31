package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.{LogEntry, Snapshot}

case class InstallSnapshot(snapshot: Snapshot, lastEntry: LogEntry)
