package com.github.punctuality.raft.service

import com.github.punctuality.raft.model.Node
import com.github.punctuality.raft.protocol.AppendEntriesResponse

/** Policy on how to propagate logs to other nodes
  */
trait LogPropagator[F[_]] {

  /** Propagate logs from current node (leader)
    * @param peerId Id of receiving node
    * @param term Current RAFT term
    * @param nextIndex Index to pick latter logs
    * @return [[AppendEntriesResponse]] contains status of logs propagation
    */
  def propagateLogs(peerId: Node, term: Long, nextIndex: Long): F[AppendEntriesResponse]

}
