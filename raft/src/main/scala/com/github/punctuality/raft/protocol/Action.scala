package com.github.punctuality.raft.protocol

import com.github.punctuality.raft.model.Node

sealed trait Action

case class RequestForVote(peerId: Node, request: VoteRequest)      extends Action
case class ReplicateLog(peerId: Node, term: Long, nextIndex: Long) extends Action
case class CommitLogs(matchIndex: Map[Node, Long])                 extends Action
case class AnnounceLeader(leaderId: Node, resetPrevious: Boolean)  extends Action
case object ResetLeaderAnnouncer                                   extends Action
case object StoreState                                             extends Action
