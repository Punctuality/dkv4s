package com.github.punctuality.dkv4s.raft.protocol

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], lastAppliedIndex: Long = 0)
