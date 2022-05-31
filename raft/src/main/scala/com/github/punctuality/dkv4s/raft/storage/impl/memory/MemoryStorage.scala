package com.github.punctuality.dkv4s.raft.storage.impl.memory

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.effect.Async
import com.github.punctuality.dkv4s.raft.storage.Storage

object MemoryStorage {
  def empty[F[_]: Async]: F[Storage[F]] =
    for {
      snapshotStorage <- MemorySnapshotStorage.empty[F]
      stateStorage    <- MemoryStateStorage.empty[F]
      logStorage      <- MemoryLogStorage.empty[F]
    } yield Storage[F](logStorage, stateStorage, snapshotStorage)
}
