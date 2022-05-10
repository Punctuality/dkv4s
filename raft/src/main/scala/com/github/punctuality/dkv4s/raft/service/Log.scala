package com.github.punctuality.dkv4s.raft.service

import cats.effect.Deferred
import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.protocol.{AppendEntries, LogState}
import com.github.punctuality.dkv4s.raft.storage._

/** Log entity. Stores events, manages state, applies commands
  */
trait Log[F[_]] {

  val logStorage: LogStorage[F]

  val snapshotStorage: SnapshotStorage[F]

  val stateMachine: StateMachine[F]

  val clusterConfigStorage: ClusterConfigStorage[F]

  val compactionPolicy: LogCompactionPolicy[F]

  def transactional[A](t: => F[A]): F[A]

  /** @return Last committed index
    */
  def getCommitIndex: F[Long]

  /** Updated committed index
    * @param index new committed index
    */
  def setCommitIndex(index: Long): F[Unit]

  /** Initialize log and state machine state:
    *    1. Updates state machine up to latest snapshot
    *    2. Sets new commit index
    *    3. Applies commands from the log
    */
  def initialize: F[Unit]

  /** Composes current log state
    * @return Up to date [[LogState]]
    */
  def state: F[LogState]

  /** Get log entry by index
    * @param index specified index
    * @return optional log entry for index [[LogEntry]]
    */
  def get(index: Long): F[Option[LogEntry]]

  /** Apply read command
    * @param command Defined [[ReadCommand]]
    * @tparam T Expected command result
    * @return result of the command, specified by type
    */
  def applyReadCommand[T](command: ReadCommand[T]): F[T]

  /** Get all log entries which have higher or equal log index
    * @param leaderId Leader Node id
    * @param term Current RAFT term
    * @param nextIndex Index for start collection of entries
    * @return [[AppendEntries]] with all entries
    */
  def getAppendEntries(leaderId: Node, term: Long, nextIndex: Long): F[AppendEntries]

  /** Appends new command to log
    * and puts it in "awaiting commit" set
    * @param term Current RAFT term
    * @param command Command to process
    * @param deferred Supplied deferred, which will be completed after command's commit
    * @tparam T Command result type
    * @return New [[LogEntry]]
    */
  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry]

  /** Appends new entries to the log, with respect to already applied by leader logs
    * Afterwards checks if compaction is needed
    * @param entries New log entries
    * @param leaderPrevLogIndex Leader's previous last log index
    * @param leaderCommit Leader's last commit
    * @return [[Boolean]] on whether there were any commits made
    */
  def appendEntries(entries: List[LogEntry],
                    leaderPrevLogIndex: Long,
                    leaderCommit: Long
  ): F[Boolean]

  /** Commit logs, based on nodes quorum
    * @param matchIndex State of every node index
    * @return [[Boolean]] on whether there were any commits made
    */
  def commitLogs(matchIndex: Map[Node, Long]): F[Boolean]

  /** @return Optional latest [[Snapshot]]
    */
  def latestSnapshot: F[Option[Snapshot]]

  /** Apply snapshot to the logs state
    * @param snapshot Snapshot to apply
    * @param lastEntry Last entry from snapshot ???
    */
  def installSnapshot(snapshot: Snapshot, lastEntry: LogEntry): F[Unit]
}
