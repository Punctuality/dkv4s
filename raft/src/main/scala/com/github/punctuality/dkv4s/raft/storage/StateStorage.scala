package com.github.punctuality.dkv4s.raft.storage

import com.github.punctuality.dkv4s.raft.model.PersistedState

trait StateStorage[F[_]] {

  def persistState(state: PersistedState): F[Unit]

  def retrieveState(): F[Option[PersistedState]]
}
