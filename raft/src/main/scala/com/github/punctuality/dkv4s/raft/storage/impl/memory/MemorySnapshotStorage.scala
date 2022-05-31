package com.github.punctuality.dkv4s.raft.storage.impl.memory

import cats.Monad
import cats.syntax.functor._
import cats.effect.{Concurrent, Ref}
import com.github.punctuality.dkv4s.raft.model.Snapshot
import com.github.punctuality.dkv4s.raft.storage.SnapshotStorage

class MemorySnapshotStorage[F[_]: Monad](ref: Ref[F, Option[Snapshot]]) extends SnapshotStorage[F] {

  override def saveSnapshot(snapshot: Snapshot): F[Unit] = ref.set(Some(snapshot))

  override def retrieveSnapshot: F[Option[Snapshot]] = ref.get

  override def getLatestSnapshot: F[Option[Snapshot]] = ref.get
}

object MemorySnapshotStorage {
  def empty[F[_]: Concurrent]: F[MemorySnapshotStorage[F]] =
    Ref.of[F, Option[Snapshot]](None).map(new MemorySnapshotStorage[F](_))
}
