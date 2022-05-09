package com.github.punctuality.raft.storage

import com.github.punctuality.raft.model.LogEntry

/** A place where to store logs (commands)
  */
trait LogStorage[F[_]] {

  /** Retrieve last log's index
    * @return last index
    */
  def lastIndex: F[Long]

  /** Get [[LogEntry]] by it's index
    * @param index index of the log
    * @return Log entry [[LogEntry]] or null
    *
    * TODO Improve this (do not return null)
    */
  def get(index: Long): F[LogEntry]

  /** Put [[LogEntry]] to log storage by index
    * @param index index of new entry
    * @param logEntry Log entry
    * @return identity of logEntry
    */
  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  /** Delete log applied before provided index
    * @param index predicate index to filter by
    */
  def deleteBefore(index: Long): F[Unit]

  /** Delete log applied after provided index
    * @param index predicate index to filter by
    */
  def deleteAfter(index: Long): F[Unit]
}
