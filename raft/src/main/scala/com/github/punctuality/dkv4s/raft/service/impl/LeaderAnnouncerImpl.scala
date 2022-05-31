package com.github.punctuality.dkv4s.raft.service.impl

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.effect.{Concurrent, Deferred, Ref}
import com.github.punctuality.dkv4s.raft.model.Node
import com.github.punctuality.dkv4s.raft.service.LeaderAnnouncer
import com.github.punctuality.dkv4s.raft.util.Logger

class LeaderAnnouncerImpl[F[_]: Monad: Concurrent: Logger](
  val deferredRef: Ref[F, Deferred[F, Node]]
) extends LeaderAnnouncer[F] {

  def announce(leader: Node): F[Unit] =
    Logger[F].info(s"A new leader is elected among the members. New Leader is '$leader'.") >>
      deferredRef.get.flatMap(_.complete(leader)).void

  def reset(): F[Unit] =
    Logger[F].debug("Resetting the Announcer.") >>
      Deferred[F, Node].flatMap(deferredRef.set)

  def listen(): F[Node] =
    deferredRef.get.flatMap(_.get)
}

object LeaderAnnouncerImpl {
  def build[F[_]: Monad: Concurrent: Logger]: F[LeaderAnnouncerImpl[F]] =
    Ref.ofEffect(Deferred[F, Node]).map(new LeaderAnnouncerImpl[F](_))

}
