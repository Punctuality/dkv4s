package com.github.punctuality.dkv4s.raft.model

import cats.Applicative
import cats.syntax.applicative._
import com.github.punctuality.dkv4s.raft.protocol.LogState
import com.github.punctuality.dkv4s.raft.service.Log
import com.github.punctuality.dkv4s.raft.storage.StateMachine

/** Policy which defines when to apply compaction stage
  */
trait LogCompactionPolicy[F[_]] {

  /** Check whether should compaction start
    * @param state Current state of [[Log]]
    * @param stateMachine Current state of [[StateMachine]]
    * @return
    */
  def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean]
}

object LogCompactionPolicy {

  def noCompaction[F[_]: Applicative]: LogCompactionPolicy[F] =
    (_: LogState, _: StateMachine[F]) => false.pure[F]

  def fixedSize[F[_]: Applicative](logsCount: Int): LogCompactionPolicy[F] =
    (state: LogState, _: StateMachine[F]) =>
      (state.lastAppliedIndex > logsCount && state.lastAppliedIndex % logsCount == 0).pure[F]
}
