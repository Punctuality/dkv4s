package com.github.punctuality.dkv4s.raft.rpc.grpc.serializer

import com.google.protobuf.ByteString

trait ProtoSerializer[T] {
  def encode(obj: T): ByteString
  def decode(byteString: ByteString): T
}

object ProtoSerializer {
  def apply[T](implicit ev: ProtoSerializer[T]): ProtoSerializer[T] = ev
}
