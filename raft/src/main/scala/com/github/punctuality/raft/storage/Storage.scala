package com.github.punctuality.raft.storage

case class Storage[F[_]](logStorage: LogStorage[F],
                         stateStorage: StateStorage[F],
                         snapshotStorage: SnapshotStorage[F]
)
