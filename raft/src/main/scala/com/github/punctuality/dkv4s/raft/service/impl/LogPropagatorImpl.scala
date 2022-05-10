package com.github.punctuality.dkv4s.raft.service.impl

import cats.effect.{Concurrent, Ref}
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.punctuality.dkv4s.raft.model.{Node, Snapshot}
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.service.LogPropagator
import com.github.punctuality.dkv4s.raft.util.Logger

import scala.collection.Set

class LogPropagatorImpl[F[_]: Concurrent: Logger](leaderId: Node,
                                                  log: LogImpl[F],
                                                  clients: RpcClientManagerImpl[F],
                                                  installingRef: Ref[F, Set[Node]]
) extends LogPropagator[F] {

  def propagateLogs(peerId: Node, term: Long, nextIndex: Long): F[AppendEntriesResponse] =
    for {
      _        <- Logger[F].trace(s"Replicating logs to $peerId. Term: $term, nextIndex: $nextIndex")
      _        <- snapshotIsNotInstalling(peerId)
      snapshot <- log.latestSnapshot
      _        <- Logger[F].trace(s"Latest snapshot: $snapshot")
      response <-
        if (snapshot.exists(_.lastIndex >= nextIndex)) sendSnapshot(peerId, snapshot.get)
        else log.getAppendEntries(leaderId, term, nextIndex).flatMap(clients.sendEntries(peerId, _))
    } yield response

  private def sendSnapshot(peerId: Node, snapshot: Snapshot): F[AppendEntriesResponse] = {
    val response = for {
      _        <- Logger[F].trace(s"Installing an Snapshot for peer $peerId, snapshot: $snapshot")
      _        <- installingRef.update(_ + peerId)
      logEntry <- log.get(snapshot.lastIndex).map(_.get)
      response <- clients.sendSnapshot(peerId, InstallSnapshot(snapshot, logEntry))
      _        <- Logger[F].trace(s"Response after installing snapshot $response")
      _        <- installingRef.update(_ - peerId)
    } yield response

    Concurrent[F].onError(response) { case error =>
      Logger[F].trace(s"Error during snapshot installation $error") >>
        installingRef.update(_ - peerId)
    }
  }

  private def snapshotIsNotInstalling(peerId: Node): F[Unit] =
    installingRef.get.flatMap(set =>
      if (set.contains(peerId))
        Concurrent[F].raiseError(new RuntimeException("Client is installing snapshot"))
      else Concurrent[F].unit
    )
}

object LogPropagatorImpl {
  def build[F[_]: Concurrent: Logger](leaderId: Node,
                                      clients: RpcClientManagerImpl[F],
                                      log: LogImpl[F]
  ): F[LogPropagatorImpl[F]] =
    Ref.of[F, Set[Node]](Set.empty).map(new LogPropagatorImpl[F](leaderId, log, clients, _))
}
