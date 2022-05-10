package com.github.punctuality.dkv4s.raft.service.impl

import cats.Monad
import cats.effect.std.Semaphore
import cats.effect.{Concurrent, Deferred, MonadCancel, Ref}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.applicativeError._
import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.protocol.{AppendEntries, LogState}
import com.github.punctuality.dkv4s.raft.service.{ClusterConfigStorage, Log}
import com.github.punctuality.dkv4s.raft.storage.{LogStorage, SnapshotStorage, StateMachine}
import com.github.punctuality.dkv4s.raft.util.Logger

import scala.collection.concurrent.TrieMap

class LogImpl[F[_]: MonadCancel[*[_], Throwable]: Logger](
  val logStorage: LogStorage[F],
  val snapshotStorage: SnapshotStorage[F],
  val stateMachine: StateMachine[F],
  val clusterConfigStorage: ClusterConfigStorage[F],
  val compactionPolicy: LogCompactionPolicy[F],
  commitIndexRef: Ref[F, Long],
  semaphore: Semaphore[F]
) extends Log[F] {

  // TODO Deal with this Any type
  private val awaitingCommands = TrieMap[Long, Deferred[F, Any]]()

  override def transactional[A](code: => F[A]): F[A] =
    semaphore.permit.use(_ => code)

  override def getCommitIndex: F[Long] =
    commitIndexRef.get

  override def setCommitIndex(index: Long): F[Unit] =
    Logger[F].trace(s"Set commit index to $index") >>
      commitIndexRef.set(index)

  def initialize: F[Unit] =
    transactional {
      for {
        _                 <- Logger[F].debug("Initializing log")
        snapshot          <- latestSnapshot
        _                 <- snapshot.map(restoreSnapshot).getOrElse(Monad[F].unit)
        commitIndex       <- getCommitIndex
        _                 <- Logger[F].debug(s"Latest committed index $commitIndex")
        stateMachineIndex <- stateMachine.appliedIndex
        _                 <- Logger[F].debug(s"State machine applied index $stateMachineIndex")
        _ <-
          if (stateMachineIndex > commitIndex) setCommitIndex(stateMachineIndex)
          else (stateMachineIndex + 1 to commitIndex).toList.traverse(commit).void
      } yield ()
    }

  def state: F[LogState] =
    for {
      lastIndex <- logStorage.lastIndex
      lastTerm <-
        if (lastIndex > 0) logStorage.get(lastIndex).map(_.map(_.term))
        else Monad[F].pure(None)
      commitIndex <- getCommitIndex
    } yield LogState(lastIndex, lastTerm, commitIndex)

  def get(index: Long): F[Option[LogEntry]] =
    logStorage.get(index)

  def applyReadCommand[T](command: ReadCommand[T]): F[T] =
    if (stateMachine.applyRead.isDefinedAt(command))
      stateMachine.applyRead(command).asInstanceOf[F[T]]
    else new RuntimeException("Could not run the command").raiseError

  def getAppendEntries(leaderId: Node, term: Long, nextIndex: Long): F[AppendEntries] =
    for {
      _         <- Logger[F].trace(s"Getting append entries since $nextIndex")
      lastIndex <- logStorage.lastIndex
      lastEntry <-
        if (nextIndex > 1) logStorage.get(nextIndex - 1) else Monad[F].pure(None)
      commitIndex <- getCommitIndex
      entries     <- (nextIndex to lastIndex).toList.traverse(i => logStorage.get(i).map(_.get))
      prevLogIndex = lastEntry.map(_.index).getOrElse(0L)
      prevLogTerm  = lastEntry.map(_.term).getOrElse(0L)
    } yield AppendEntries(leaderId, term, prevLogIndex, prevLogTerm, commitIndex, entries)

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry] =
    transactional {
      for {
        lastIndex <- logStorage.lastIndex
        logEntry   = LogEntry(term, lastIndex + 1, command)
        _ <-
          Logger[F].trace(s"Appending a command to the log. Term: $term, Index: ${logEntry.index}")
        _ <- logStorage.put(logEntry.index, logEntry)
        _  = awaitingCommands.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
        _ <- Logger[F].trace(s"Entry appended. Term: $term, Index: ${lastIndex + 1}")
      } yield logEntry
    }

  def appendEntries(entries: List[LogEntry],
                    leaderPrevLogIndex: Long,
                    leaderCommit: Long
  ): F[Boolean] =
    transactional {
      for {
        lastIndex    <- logStorage.lastIndex
        appliedIndex <- getCommitIndex
        _            <- truncateInconsistentLogs(entries, leaderPrevLogIndex, lastIndex)
        _            <- putEntries(entries, leaderPrevLogIndex, lastIndex)
        _            <- Logger[F].trace(s"Commit index: $appliedIndex | Entries: $entries")
        committed    <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
        // FIXME Consider applying without waiting for new heartbeat️
        _ <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield committed.nonEmpty
    }

  private def truncateInconsistentLogs(entries: List[LogEntry],
                                       leaderLogSent: Long,
                                       lastLogIndex: Long
  ): F[Unit] =
    if (entries.nonEmpty && lastLogIndex > leaderLogSent) {
      Logger[F].trace(s"Truncating log entries from the Log after: $leaderLogSent") >>
        logStorage.get(lastLogIndex).flatMap {
          case Some(entry) if entry.term != entries.head.term =>
            logStorage.deleteAfter(leaderLogSent)
          case _ => Monad[F].unit
        }
    } else Monad[F].unit

  private def putEntries(entries: List[LogEntry],
                         leaderPrevLogIndex: Long,
                         logLastIndex: Long
  ): F[Unit] = {
    val logEntries =
      if ((leaderPrevLogIndex + entries.length) > logLastIndex)
        entries.drop((logLastIndex - leaderPrevLogIndex).toInt)
      else List.empty

    Logger[F].trace(s"Putting entries $logEntries to log storage") >>
      logEntries.traverse(entry => logStorage.put(entry.index, entry)).void
  }

  def commitLogs(matchIndex: Map[Node, Long]): F[Boolean] =
    transactional {
      for {
        lastIndex   <- logStorage.lastIndex
        commitIndex <- getCommitIndex
        committed   <- (commitIndex + 1 to lastIndex).toList.traverse(commitIfMatched(matchIndex, _))
        _           <- if (committed.contains(true)) compactLogs() else Monad[F].unit
      } yield committed.contains(true)
    }

  private def applyCommand(index: Long, command: Command[_]): F[Unit] = {
    val output = command match {
      case command: ClusterConfigurationCommand =>
        clusterConfigStorage.setClusterConfiguration(command.toConfig)
      case command: ReadCommand[_] =>
        stateMachine.applyRead.apply(command)
      case command: WriteCommand[_] =>
        stateMachine.applyWrite.apply((index, command))
    }

    output.flatMap { result =>
      awaitingCommands.get(index) match {
        case Some(deferred) => deferred.complete(result).map(_ => awaitingCommands.remove(index))
        case None           => Monad[F].unit
      }
    }
  }

  private def commitIfMatched(matchIndex: Map[Node, Long], index: Long): F[Boolean] =
    for {
      config <- clusterConfigStorage.getClusterConfiguration
      matched = config.quorumReached(matchIndex.filter(_._2 >= index).keySet)
      result <- if (matched) commit(index).as(true) else Monad[F].pure(false)
    } yield result

  private def commit(index: Long): F[Unit] =
    for {
      _     <- Logger[F].trace(s"Committing the log entry at index $index")
      entry <- logStorage.get(index)
      _     <- Logger[F].trace(s"Committed the log entry at index $index $entry")
      _     <- applyCommand(index, entry.get.command)
      _     <- setCommitIndex(index)
    } yield ()

  def latestSnapshot: F[Option[Snapshot]] =
    snapshotStorage.retrieveSnapshot

  def installSnapshot(snapshot: Snapshot, lastEntry: LogEntry): F[Unit] =
    transactional {
      for {
        lastIndex <- logStorage.lastIndex
        _ <-
          if (lastIndex >= snapshot.lastIndex) // TODO Introduce domain errors
            new RuntimeException("A new snapshot is already applied").raiseError
          else Monad[F].unit
        _ <- Logger[F].trace(s"Installing a snapshot, $snapshot")
        _ <- snapshotStorage.saveSnapshot(snapshot)
        _ <- Logger[F].trace("Restoring state from snapshot")
        _ <- restoreSnapshot(snapshot)
        _ <- logStorage.put(lastEntry.index, lastEntry)
        _ <- setCommitIndex(snapshot.lastIndex)
      } yield ()
    }

  private def compactLogs(): F[Unit] =
    for {
      logState <- state
      eligible <- compactionPolicy.eligible(logState, stateMachine)
      _        <- if (eligible) takeSnapshot() else Monad[F].unit
    } yield ()

  private def takeSnapshot(): F[Unit] =
    for {
      _        <- Logger[F].trace("Starting to take snapshot")
      snapshot <- stateMachine.takeSnapshot
      config   <- clusterConfigStorage.getClusterConfiguration
      _        <- snapshotStorage.saveSnapshot(Snapshot(snapshot._1, snapshot._2, config))
      _        <- Logger[F].trace(s"Snapshot is stored $snapshot")
      _        <- Logger[F].trace(s"Deleting logs before ${snapshot._1}")
      _        <- logStorage.deleteBefore(snapshot._1)
      _        <- Logger[F].trace(s"Logs before ${snapshot._1} are deleted.")
    } yield ()

  private def restoreSnapshot(snapshot: Snapshot): F[Unit] =
    Logger[F].trace(
      s"Restoring Snapshot(index: ${snapshot.lastIndex}), Config: ${snapshot.config}"
    ) >>
      clusterConfigStorage.setClusterConfiguration(snapshot.config) >>
      stateMachine.restoreSnapshot(snapshot.lastIndex, snapshot.bytes) >>
      Logger[F].trace("Snapshot is restored.")
}

object LogImpl {
  def build[F[_]: Concurrent: Logger](logStorage: LogStorage[F],
                                      snapshotStorage: SnapshotStorage[F],
                                      stateMachine: StateMachine[F],
                                      compactionPolicy: LogCompactionPolicy[F],
                                      membershipManager: ClusterConfigStorage[F],
                                      lastCommitIndex: Long
  ): F[LogImpl[F]] =
    for {
      lock           <- Semaphore[F](1)
      commitIndexRef <- Ref.of[F, Long](lastCommitIndex)
    } yield new LogImpl(
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      compactionPolicy,
      commitIndexRef,
      lock
    )
}
