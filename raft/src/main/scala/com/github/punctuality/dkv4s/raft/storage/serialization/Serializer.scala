package com.github.punctuality.dkv4s.raft.storage.serialization

import scala.annotation.implicitNotFound

@implicitNotFound("""
Could not find an instance of Serializer for ${T}.
You might add a custom serializer for ${T}, or just import the default one.

import com.github.punctuality.raft.storage.serialization.default._
""")
trait Serializer[T] {
  def toBytes(obj: T): Array[Byte]

  def fromBytes(bytes: Array[Byte]): Option[T]
}

object Serializer {
  def apply[A](implicit instance: Serializer[A]): Serializer[A] = instance
}
