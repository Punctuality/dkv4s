package com.github.punctuality.dkv4s.raft.node

import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.protocol._

/** Node state, describing current state and appropriate actions
  */
trait NodeState {

  /** Transition applied on election timer timing out
    * @param logState Current log state, generated by log storage
    * @param config Current cluster configuration
    * @return Next node state and actions to achieve it
    */
  def onElectionTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action])

  def onVoteRequest(logState: LogState,
                    config: ClusterConfiguration,
                    msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action]))

  def onEntries(state: LogState,
                config: ClusterConfiguration,
                msg: AppendEntries,
                localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action]))

  def onVoteResponse(logState: LogState,
                     config: ClusterConfiguration,
                     msg: VoteResponse
  ): (NodeState, List[Action])

  def onEntriesResp(logState: LogState,
                    config: ClusterConfiguration,
                    msg: AppendEntriesResponse
  ): (NodeState, List[Action])

  def onReplicateLog(config: ClusterConfiguration): List[Action]

  def onSnapshotInstalled(logState: LogState,
                          config: ClusterConfiguration
  ): (NodeState, AppendEntriesResponse)

  /** @return Current Leader [[Node]], can be [[None]]
    */
  def leader: Option[Node]

  /** @return Generated state to persist
    */
  def toPersistedState: PersistedState
}