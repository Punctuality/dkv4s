package com.github.punctuality.raft.model

case class LogEntry(term: Long, index: Long, command: Command[_])
