package com.github.punctuality.dkv4s.raft.rpc.grpc.serializer

import com.google.protobuf.ByteString

import java.io.{ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class JavaProtoSerializer[T] extends ProtoSerializer[T] {

  def encode(obj: T): ByteString = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close()

    ByteString.copyFrom(stream.toByteArray)
  }

  def decode(byteString: ByteString): T = {

    val ois      = new ObjectInputStream(byteString.newInput)
    val response = ois.readObject().asInstanceOf[T]
    ois.close()

    response

  }
}

object JavaProtoSerializer {
  def anySerObject[T]: JavaProtoSerializer[T] = new JavaProtoSerializer[T]
}
