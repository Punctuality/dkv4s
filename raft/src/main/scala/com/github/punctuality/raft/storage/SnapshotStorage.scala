package com.github.punctuality.raft.storage

import com.github.punctuality.raft.model.Snapshot

/** Storage of saved snapshots (compacted logs)
  */
trait SnapshotStorage[F[_]] {

  /** Save snapshot to storage
    * @param snapshot provided [[Snapshot]]
    */
  def saveSnapshot(snapshot: Snapshot): F[Unit]

  /** Retrieve snapshot from persistent storage and update local ref
    * @return Optional latest [[Snapshot]]
    */
  def retrieveSnapshot: F[Option[Snapshot]]

  /** Get local latest snapshot
    * @return Optional local [[Snapshot]]
    */
  def getLatestSnapshot: F[Option[Snapshot]]
}
