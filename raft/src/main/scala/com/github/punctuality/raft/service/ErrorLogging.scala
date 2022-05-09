package com.github.punctuality.raft.service

import cats.MonadError
import com.github.punctuality.raft.util.Logger

trait ErrorLogging[F[_]] {

  def errorLogging[A](
    message: String
  )(fa: F[A])(implicit L: Logger[F], ME: MonadError[F, Throwable]): F[A] =
    ME.attemptTap(fa) {
      case Left(error) =>
        L.warn(s"Error in ($message):  $error")
      case Right(_) => ME.pure(())
    }
}
