package com.github.punctuality.dkv4s.raft.node

import com.github.punctuality.dkv4s.raft.model.{LogEntry, Node, PersistedState}
import com.github.punctuality.dkv4s.raft.protocol
import com.github.punctuality.dkv4s.raft.protocol.{Action, AnnounceLeader, AppendEntries, AppendEntriesResponse, ClusterConfiguration, LogState, ReplicateLog, RequestForVote, StoreState, VoteRequest, VoteResponse}
import com.github.punctuality.raft.protocol._

case class CandidateNode(currentNode: Node,
                         currentTerm: Long,
                         lastTerm: Long,
                         votedFor: Option[Node]   = None,
                         votedReceived: Set[Node] = Set.empty
) extends NodeState {

  private def announceLeader(otherNodes: Set[Node],
                             logState: LogState
  ): (LeaderNode, List[Action]) = {
    val matchIndex: Map[Node, Long] = otherNodes.map(n => (n, 0L)).toMap
    val nextIndex: Map[Node, Long]  = otherNodes.map(n => (n, logState.lastLogIndex + 1)).toMap
    val actions: List[ReplicateLog] =
      otherNodes.map(n => ReplicateLog(n, currentTerm, logState.lastLogIndex + 1)).toList

    (
      LeaderNode(currentNode, currentTerm, matchIndex, nextIndex),
      StoreState :: AnnounceLeader(currentNode, resetPrevious = false) :: actions
    )
  }

  override def onElectionTimer(logState: LogState,
                               config: ClusterConfiguration
  ): (NodeState, List[Action]) = {
    val electionTerm = currentTerm + 1
    val lastTerm_    = logState.lastLogTerm.getOrElse(lastTerm)
    val request      = VoteRequest(currentNode, electionTerm, logState.lastLogIndex, lastTerm_)

    val otherNodes = config.members.filterNot(_ == currentNode)
    val actions    = otherNodes.toList.map(nodeId => RequestForVote(nodeId, request))

    if (config.members.size == 1) announceLeader(otherNodes, logState)
    else {
      (
        this.copy(
          currentTerm   = electionTerm,
          lastTerm      = lastTerm_,
          votedFor      = Some(currentNode),
          votedReceived = Set(currentNode)
        ),
        StoreState :: actions
      )
    }
  }

  override def onVoteRequest(logState: LogState,
                             config: ClusterConfiguration,
                             msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {
    val myLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOK =
      (msg.lastLogTerm > myLogTerm) || (msg.lastLogTerm == myLogTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK =
      (msg.term > currentTerm) || (msg.term == currentTerm && (votedFor.isEmpty || votedFor
        .contains(msg.nodeId)))

    if (logOK && termOK) {
      (
        FollowerNode(currentNode, msg.term, Some(msg.nodeId), None),
        (VoteResponse(currentNode, msg.term, voteGranted = true), List(StoreState))
      )
    } else {
      (this, (VoteResponse(currentNode, currentTerm, voteGranted = false), List.empty))
    }
  }

  override def onVoteResponse(logState: LogState,
                              config: ClusterConfiguration,
                              msg: VoteResponse
  ): (NodeState, List[Action]) = {
    val votedReceived_ = if (msg.voteGranted) votedReceived + msg.nodeId else votedReceived
    val quorumSize     = (config.members.size + 1) / 2

    if (msg.term > currentTerm)
      (FollowerNode(currentNode, msg.term), List(StoreState))
    else if (msg.term == currentTerm && msg.voteGranted && votedReceived_.size >= quorumSize)
      announceLeader(config.members.filterNot(_ == currentNode), logState)
    else (this.copy(votedReceived = votedReceived_), List.empty)
  }

  override def onEntries(logState: LogState,
                         config: ClusterConfiguration,
                         msg: AppendEntries,
                         localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      (
        this,
        (
          protocol.AppendEntriesResponse(
            currentNode,
            currentTerm,
            msg.prevLogIndex,
            success = false
          ),
          List.empty
        )
      )
    } else if (msg.term > currentTerm) {

      val nextState = FollowerNode(currentNode, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = false))

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex,
              success = false
            ),
            actions
          )
        )
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm)
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex,
              success = false
            ),
            actions
          )
        )
      else
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex + msg.entries.length,
              success = true
            ),
            actions
          )
        )
    } else {

      val nextState = FollowerNode(currentNode, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = false))

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex,
              success = false
            ),
            actions
          )
        )
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm) {
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex,
              success = false
            ),
            actions
          )
        )
      } else
        (
          nextState,
          (
            protocol.AppendEntriesResponse(
              currentNode,
              msg.term,
              msg.prevLogIndex + msg.entries.length,
              success = true
            ),
            actions
          )
        )
    }

  override def onEntriesResp(logState: LogState,
                             cluster: ClusterConfiguration,
                             msg: AppendEntriesResponse
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(config: ClusterConfiguration): List[Action] =
    List.empty

  override def leader: Option[Node] =
    None

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)

  override def onSnapshotInstalled(logState: LogState,
                                   cluster: ClusterConfiguration
  ): (NodeState, AppendEntriesResponse) =
    (
      this,
      protocol.AppendEntriesResponse(
        currentNode,
        currentTerm,
        logState.lastAppliedIndex,
        success = false
      )
    )
}
