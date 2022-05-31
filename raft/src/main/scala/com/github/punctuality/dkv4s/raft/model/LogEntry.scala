package com.github.punctuality.dkv4s.raft.model

case class LogEntry(term: Long, index: Long, command: Command[_])
