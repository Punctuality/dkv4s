package com.github.punctuality.raft.storage

import com.github.punctuality.raft.model.PersistedState

trait StateStorage[F[_]] {

  def persistState(state: PersistedState): F[Unit]

  def retrieveState(): F[Option[PersistedState]]
}
