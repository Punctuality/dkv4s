package com.github.punctuality.dkv4s.raft.node

import com.github.punctuality.dkv4s.raft.model.{LogEntry, Node, PersistedState}
import com.github.punctuality.dkv4s.raft.protocol._

case class LeaderNode(currrentNode: Node,
                      currentTerm: Long,
                      matchIndex: Map[Node, Long],
                      nextIndex: Map[Node, Long]
) extends NodeState {

  override def onElectionTimer(logState: LogState,
                               config: ClusterConfiguration
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onVoteRequest(logState: LogState,
                             config: ClusterConfiguration,
                             msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {
    val lastTerm = logState.lastLogTerm.getOrElse(currentTerm)
    val logOK =
      (msg.lastLogTerm > lastTerm) || (msg.lastLogTerm == lastTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK = msg.term > currentTerm

    if (logOK && termOK)
      FollowerNode(currrentNode, msg.term, Some(msg.nodeId)) -> (VoteResponse(
        currrentNode,
        msg.term,
        voteGranted = true
      ) -> List(StoreState, ResetLeaderAnnouncer))
    else {
      val nextIndex_  = nextIndex + (msg.nodeId  -> (msg.lastLogIndex + 1))
      val matchIndex_ = matchIndex + (msg.nodeId -> msg.lastLogIndex)

      this.copy(nextIndex = nextIndex_, matchIndex = matchIndex_) -> (VoteResponse(
        currrentNode,
        currentTerm,
        voteGranted = false
      ) -> List(ReplicateLog(msg.nodeId, currentTerm, msg.lastLogIndex + 1)))
    }
  }

  override def onVoteResponse(logState: LogState,
                              config: ClusterConfiguration,
                              msg: VoteResponse
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onEntries(state: LogState,
                         config: ClusterConfiguration,
                         msg: AppendEntries,
                         localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      this -> (AppendEntriesResponse(
        currrentNode,
        currentTerm,
        msg.prevLogIndex,
        success = false
      ) -> List.empty)
    } else if (msg.term > currentTerm) {

      val nextState = FollowerNode(currrentNode, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = true))

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex,
          success = false
        ) -> actions)
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm)
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex,
          success = false
        ) -> actions)
      else
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex + msg.entries.length,
          success = true
        ) -> actions)
    } else {

      val nextState = FollowerNode(currrentNode, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, resetPrevious = true))

      if (msg.prevLogTerm > 0 && localPrvLogEntry.isEmpty)
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex,
          success = false
        ) -> actions)
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm) {
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex,
          success = false
        ) -> actions)
      } else
        nextState -> (AppendEntriesResponse(
          currrentNode,
          msg.term,
          msg.prevLogIndex + msg.entries.length,
          success = true
        ) -> actions)
    }

  override def onEntriesResp(logState: LogState,
                             cluster: ClusterConfiguration,
                             msg: AppendEntriesResponse
  ): (NodeState, List[Action]) =
    if (msg.currentTerm > currentTerm) {
      FollowerNode(currrentNode, msg.currentTerm, None, None) -> List(
        StoreState,
        ResetLeaderAnnouncer
      )
    } else {
      if (msg.success) {
        val nextIndex_  = nextIndex + (msg.nodeId  -> (msg.ack + 1L))
        val matchIndex_ = matchIndex + (msg.nodeId -> msg.ack)

        this.copy(matchIndex = matchIndex_, nextIndex = nextIndex_) -> List(
          CommitLogs(matchIndex_ + (currrentNode -> logState.lastLogIndex))
        )
      } else {
        //If AppendEntries fails because of log inconsistency - decrement nextIndex and retry
        val nodeNextIndex = nextIndex.get(msg.nodeId) match {
          case Some(next) if next == 1 => 1
          case Some(next)              => next - 1
          case None                    => 1
        }
        val newNextIndex = nextIndex + (msg.nodeId -> nodeNextIndex)

        this.copy(nextIndex = newNextIndex) -> List(
          ReplicateLog(msg.nodeId, currentTerm, newNextIndex(msg.nodeId))
        )
      }
    }

  override def onReplicateLog(cluster: ClusterConfiguration): List[Action] =
    cluster.members
      .filterNot(_ == currrentNode)
      .map(peer => ReplicateLog(peer, currentTerm, nextIndex.getOrElse(peer, 1L)))
      .toList

  override def leader: Option[Node] =
    Some(currrentNode)

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, Some(currrentNode))

  override def onSnapshotInstalled(logState: LogState,
                                   cluster: ClusterConfiguration
  ): (NodeState, AppendEntriesResponse) =
    (
      this,
      AppendEntriesResponse(currrentNode, currentTerm, logState.lastLogIndex - 1, success = false)
    )

}
