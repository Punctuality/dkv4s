package com.github.punctuality.dkv4s.raft.storage.impl.memory

import cats.Monad
import cats.syntax.functor._
import cats.effect.{Concurrent, Ref}
import com.github.punctuality.dkv4s.raft.model.PersistedState
import com.github.punctuality.dkv4s.raft.storage.StateStorage

class MemoryStateStorage[F[_]: Monad](ref: Ref[F, Option[PersistedState]]) extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] =
    ref.set(Some(state))

  override def retrieveState(): F[Option[PersistedState]] =
    ref.get

}

object MemoryStateStorage {
  def empty[F[_]: Concurrent]: F[MemoryStateStorage[F]] =
    Ref
      .of[F, Option[PersistedState]](Some(PersistedState(0, None, 0L)))
      .map(new MemoryStateStorage[F](_))
}
