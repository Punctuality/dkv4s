package com.github.punctuality.dkv4s.raft.service

import com.github.punctuality.dkv4s.raft.model.Node
import com.github.punctuality.dkv4s.raft.protocol.AppendEntriesResponse

/** Policy on how to propagate logs to other nodes
  */
trait LogPropagator[F[_]] {

  /** Propagate logs from current node (leader)
    * @param raftId Id of current Raft Group
    * @param peerId Id of receiving node
    * @param term Current RAFT term
    * @param nextIndex Index to pick latter logs
    * @return [[AppendEntriesResponse]] contains status of logs propagation
    */
  def propagateLogs(raftId: Int,
                    peerId: Node,
                    term: Long,
                    nextIndex: Long
  ): F[AppendEntriesResponse]

}
