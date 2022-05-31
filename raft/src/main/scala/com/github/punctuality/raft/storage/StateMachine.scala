package com.github.punctuality.raft.storage

import com.github.punctuality.raft.model.{ReadCommand, WriteCommand}

import java.nio.ByteBuffer

/** De-facto main storage, where logs are applied (i.e. DB engine)
  */
trait StateMachine[F[_]] {
  // TODO Think about this partial function and Any types

  /** Partially defined handler for [[WriteCommand]]
    *
    * @return Result of writing command
    */
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), F[Any]]

  /** Partially defined handler for [[ReadCommand]]
    * @return Result of reading command
    */
  def applyRead: PartialFunction[ReadCommand[_], F[Any]]

  /** @return Index of last applied log
    */
  def appliedIndex: F[Long]

  /** Get snapshot info for compaction
    * @return Last stored index [[Long]] and binary data of current state [[ByteBuffer]]
    */
  def takeSnapshot: F[(Long, ByteBuffer)]

  /** Applies snapshot to the state machine
    * @param index new last applied index
    * @param bytes data to restore from
    */
  def restoreSnapshot(index: Long, bytes: ByteBuffer): F[Unit]
}
