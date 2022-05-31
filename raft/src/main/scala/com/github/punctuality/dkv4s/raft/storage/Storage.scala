package com.github.punctuality.dkv4s.raft.storage

case class Storage[F[_]](logStorage: LogStorage[F],
                         stateStorage: StateStorage[F],
                         snapshotStorage: SnapshotStorage[F]
)
