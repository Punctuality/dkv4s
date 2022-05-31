package com.github.punctuality.raft.storage.impl.memory

import cats.Monad
import com.github.punctuality.raft.model.PersistedState
import com.github.punctuality.raft.storage.StateStorage

class MemoryStateStorage[F[_]: Monad] extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] =
    Monad[F].unit

  override def retrieveState(): F[Option[PersistedState]] =
    Monad[F].pure(Some(PersistedState(0, None, 0L)))

}

object MemoryStateStorage {
  def empty[F[_]: Monad]: F[MemoryStateStorage[F]] =
    Monad[F].pure(new MemoryStateStorage[F])
}
