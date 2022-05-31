package com.github.punctuality.dkv4s.raft.storage.impl.memory

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.effect.{Ref, Sync}
import com.github.punctuality.dkv4s.raft.model.LogEntry
import com.github.punctuality.dkv4s.raft.storage.LogStorage

class MemoryLogStorage[F[_]: Sync](itemsRef: Ref[F, Map[Long, LogEntry]],
                                   lastIndexRef: Ref[F, Long]
) extends LogStorage[F] {

  override def lastIndex: F[Long] =
    lastIndexRef.get

  override def get(index: Long): F[Option[LogEntry]] =
    itemsRef.get.map(_.get(index))

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    itemsRef.update(_ + (index -> logEntry)) >>
      lastIndexRef.update(i => Math.max(i, index)) as logEntry

  override def deleteBefore(index: Long): F[Unit] =
    itemsRef.update(items => items.filter(_._1 >= index))

  override def deleteAfter(index: Long): F[Unit] =
    itemsRef.update(items => items.filter(_._1 <= index))

}

object MemoryLogStorage {
  def empty[F[_]: Sync]: F[MemoryLogStorage[F]] =
    for {
      items <- Ref.of[F, Map[Long, LogEntry]](Map.empty)
      last  <- Ref.of[F, Long](0L)
    } yield new MemoryLogStorage[F](items, last)
}
