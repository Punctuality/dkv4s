package com.github.punctuality.dkv4s.raft.storage.impl.file

import cats.effect.Sync
import cats.{Monad, MonadThrow}
import com.github.punctuality.dkv4s.raft.model.PersistedState
import com.github.punctuality.dkv4s.raft.service.ErrorLogging
import com.github.punctuality.dkv4s.raft.storage.StateStorage
import com.github.punctuality.dkv4s.raft.storage.serialization.Serializer
import com.github.punctuality.dkv4s.raft.util.Logger

import java.nio.file.{Files, Path}
import scala.util.Try

class FileStateStorage[F[_]: MonadThrow: Logger](path: Path)(implicit S: Serializer[PersistedState])
  extends StateStorage[F] with ErrorLogging[F] {
  override def persistState(state: PersistedState): F[Unit] =
    errorLogging("Error in Persisting State") {
      Monad[F].pure {
        Files.write(path, S.toBytes(state))
        ()
      }
    }

  override def retrieveState(): F[Option[PersistedState]] =
    errorLogging("Error in retrieving state from File") {
      Monad[F].pure {
        Try(Files.readAllBytes(path)).toOption.flatMap(S.fromBytes)
      }
    }
}

object FileStateStorage {
  def open[F[_]: Sync: Logger](path: Path)(implicit S: Serializer[PersistedState]) =
    new FileStateStorage[F](path)
}
