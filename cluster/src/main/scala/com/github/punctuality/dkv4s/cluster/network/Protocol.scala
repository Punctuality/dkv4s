package com.github.punctuality.dkv4s.cluster.network

import scodec.bits.ByteVector
import scodec.codecs._

object Protocol {
  sealed trait StorageCommand
  object StorageCommand {
    implicit val discriminated: Discriminated[StorageCommand, Int] = Discriminated(uint8)
  }
  case class Put(key: ByteVector, value: ByteVector) extends StorageCommand
  object Put {
    implicit val discriminator: Discriminator[StorageCommand, Put, Int] = Discriminator(1)
  }
  case class Delete(key: ByteVector) extends StorageCommand
  object Delete {
    implicit val discriminator: Discriminator[StorageCommand, Delete, Int] = Discriminator(2)
  }
}
