package com.github.punctuality.dkv4s.cluster

import com.github.punctuality.dkv4s.raft.model.{Command, ReadCommand, WriteCommand}
import scodec.bits.ByteVector
import scodec.codecs.Discriminator

package object rocks {
  sealed trait RocksCommand
  object RocksCommand {
    val prefix = 0x10
  }

  case class WriteSingleCommand(key: ByteVector, value: ByteVector)
    extends WriteCommand[Unit] with RocksCommand
  case class WriteManyCommand(many: List[(ByteVector, ByteVector)])
    extends WriteCommand[Unit] with RocksCommand
  case class DeleteSingleCommand(key: ByteVector)      extends WriteCommand[Unit] with RocksCommand
  case class DeleteManyCommand(keys: List[ByteVector]) extends WriteCommand[Unit] with RocksCommand
  case class ReadSingleCommand(key: ByteVector)
    extends ReadCommand[Option[ByteVector]] with RocksCommand
  case class ReadManyCommand(keys: List[ByteVector])
    extends ReadCommand[List[Option[ByteVector]]] with RocksCommand

  object WriteSingleCommand {
    implicit def discriminator: Discriminator[Command[_], WriteSingleCommand, Int] =
      Discriminator(RocksCommand.prefix & 1)
  }
  object WriteManyCommand {
    implicit def discriminator: Discriminator[Command[_], WriteManyCommand, Int] =
      Discriminator(RocksCommand.prefix & 2)
  }
  object DeleteSingleCommand {
    implicit def discriminator: Discriminator[Command[_], DeleteSingleCommand, Int] =
      Discriminator(RocksCommand.prefix & 3)
  }
  object DeleteManyCommand {
    implicit def discriminator: Discriminator[Command[_], DeleteManyCommand, Int] =
      Discriminator(RocksCommand.prefix & 4)
  }
  object ReadSingleCommand {
    implicit def discriminator: Discriminator[Command[_], ReadSingleCommand, Int] =
      Discriminator(RocksCommand.prefix & 5)
  }
  object ReadManyCommand {
    implicit def discriminator: Discriminator[Command[_], ReadManyCommand, Int] =
      Discriminator(RocksCommand.prefix & 6)
  }
}
