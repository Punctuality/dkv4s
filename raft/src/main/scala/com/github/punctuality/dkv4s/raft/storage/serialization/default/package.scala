package com.github.punctuality.dkv4s.raft.storage.serialization

import com.github.punctuality.dkv4s.raft.model.LogEntry

import java.nio.ByteBuffer
import scala.util.Try

package object default {

  implicit val longSerializer: Serializer[Long] = new Serializer[Long] {
    override def toBytes(obj: Long): Array[Byte] = {
      val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
      buffer.putLong(obj)
      buffer.array()
    }

    override def fromBytes(bytes: Array[Byte]): Option[Long] =
      Try {
        val buffer = ByteBuffer.allocate(java.lang.Long.BYTES);
        buffer.put(bytes)
        buffer.flip()
        buffer.getLong()
      }.toOption
  }

  implicit val logEntrySerializer: JavaSerializer[LogEntry]       = new JavaSerializer[LogEntry]
  implicit val persistedStateSerializer: PersistedStateSerializer = new PersistedStateSerializer
}
