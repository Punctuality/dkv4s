package com.github.punctuality.raft.protocol

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], lastAppliedIndex: Long = 0)
