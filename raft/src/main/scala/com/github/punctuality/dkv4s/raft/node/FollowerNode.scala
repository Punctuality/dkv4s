package com.github.punctuality.dkv4s.raft.node

import com.github.punctuality.dkv4s.raft.model.{LogEntry, Node, PersistedState}
import com.github.punctuality.dkv4s.raft.protocol
import com.github.punctuality.dkv4s.raft.protocol.{Action, AnnounceLeader, AppendEntries, AppendEntriesResponse, ClusterConfiguration, LogState, ResetLeaderAnnouncer, StoreState, VoteRequest, VoteResponse}
import com.github.punctuality.raft.protocol._

case class FollowerNode(currentNode: Node,
                        currentTerm: Long,
                        votedFor: Option[Node]      = None,
                        currentLeader: Option[Node] = None
) extends NodeState {

  override def onElectionTimer(logState: LogState,
                               config: ClusterConfiguration
  ): (NodeState, List[Action]) =
    CandidateNode(currentNode, currentTerm, logState.lastLogTerm.getOrElse(0L))
      .onElectionTimer(logState, config) match {
      case result @ (_: LeaderNode, _)                 => result
      case (state, actions) if currentLeader.isDefined => state -> (ResetLeaderAnnouncer :: actions)
      case otherResult                                 => otherResult
    }

  override def onVoteRequest(logState: LogState,
                             config: ClusterConfiguration,
                             msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {
    if (msg.term < currentTerm) {
      (this, VoteResponse(currentNode, msg.term, voteGranted = false))
    } else if (votedFor.isEmpty || votedFor.contains(msg.nodeId)) {
      if (
        msg.lastLogIndex >= logState.lastLogIndex && msg.lastLogTerm >= logState.lastLogTerm
          .getOrElse(0L)
      ) {
        (VoteResponse(currentNode, msg.term, voteGranted = true), List(StoreState))
      } else {
        (this, (VoteResponse(currentNode, msg.term, voteGranted = false)))
      }
    }

    val myLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOK =
      (msg.lastLogTerm > myLogTerm) || (msg.lastLogTerm == myLogTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK =
      (msg.term > currentTerm) || (msg.term == currentTerm && (votedFor.isEmpty || votedFor
        .contains(msg.nodeId)))

    if (logOK && termOK)
      (
        this.copy(currentTerm = msg.term, votedFor = Some(msg.nodeId)),
        (VoteResponse(currentNode, msg.term, voteGranted = true), List(StoreState))
      )
    else
      (this, (VoteResponse(currentNode, currentTerm, voteGranted = false), List.empty))
  }

  override def onVoteResponse(logState: LogState,
                              config: ClusterConfiguration,
                              msg: VoteResponse
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onEntries(logState: LogState,
                         config: ClusterConfiguration,
                         msg: AppendEntries,
                         localPrevLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      this -> (protocol.AppendEntriesResponse(
        currentNode,
        currentTerm,
        msg.prevLogIndex,
        success = false
      ) -> List.empty)
    } else {
      if (msg.term > currentTerm) {
        val nextState = this.copy(currentTerm = msg.term, currentLeader = Some(msg.leaderId))
        val actions =
          if (currentLeader.isEmpty)
            List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = false))
          else if (currentLeader.contains(msg.leaderId))
            List(StoreState)
          else
            List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = true))

        if (msg.prevLogIndex > 0 && localPrevLogEntry.isEmpty)
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex,
            success = false
          ) -> actions)
        else if (localPrevLogEntry.isDefined && localPrevLogEntry.get.term != msg.prevLogTerm)
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex,
            success = false
          ) -> actions)
        else
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex + msg.entries.length,
            success = true
          ) -> actions)

      } else {
        val (nextState, actions) =
          if (currentLeader.isEmpty)
            this.copy(currentLeader = Some(msg.leaderId)) -> List(
              AnnounceLeader(msg.leaderId, resetPrevious = false)
            )
          else if (currentLeader.contains(msg.leaderId))
            this -> List.empty[Action]
          else
            this.copy(currentLeader = Some(msg.leaderId)) -> List(
              AnnounceLeader(msg.leaderId, resetPrevious = true)
            )

        if (msg.prevLogIndex > 0 && localPrevLogEntry.isEmpty)
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex,
            success = false
          ) -> actions)
        else if (localPrevLogEntry.isDefined && localPrevLogEntry.get.term != msg.prevLogTerm) {
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex,
            success = false
          ) -> actions)
        } else
          nextState -> (protocol.AppendEntriesResponse(
            currentNode,
            msg.term,
            msg.prevLogIndex + msg.entries.length,
            success = true
          ) -> actions)
      }
    }

  override def onEntriesResp(logState: LogState,
                             cluster: ClusterConfiguration,
                             msg: AppendEntriesResponse
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(config: ClusterConfiguration): List[Action] =
    List.empty

  override def leader: Option[Node] =
    currentLeader

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)

  override def onSnapshotInstalled(logState: LogState,
                                   config: ClusterConfiguration
  ): (NodeState, AppendEntriesResponse) =
    (
      this,
      protocol.AppendEntriesResponse(
        currentNode,
        currentTerm,
        logState.lastLogIndex - 1,
        success = true
      )
    )
}
