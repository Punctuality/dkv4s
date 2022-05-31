package com.github.punctuality.dkv4s.cluster.client

import com.github.punctuality.dkv4s.engine.codec._

trait DKV4SClient[F[_]] {

  def read[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]]
  def readMany[K: Encoder[F, *], V: Decoder[F, *]](keys: List[K]): F[List[Option[V]]]

  def write[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit]
  def writeMany[K: Encoder[F, *], V: Encoder[F, *]](values: List[(K, V)]): F[Unit]

  def delete[K: Encoder[F, *]](key: K): F[Unit]
  def deleteMany[K: Encoder[F, *]](keys: List[K]): F[Unit]

  def consistentRead[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]]
  def consistentReadMany[K: Encoder[F, *], V: Decoder[F, *]](keys: List[K]): F[List[Option[V]]]

  def consistentWrite[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit]
  def consistentWriteMany[K: Encoder[F, *], V: Encoder[F, *]](values: List[(K, V)]): F[Unit]

  def consistentDelete[K: Encoder[F, *]](key: K): F[Unit]
  def consistentDeleteMany[K: Encoder[F, *]](keys: List[K]): F[Unit]
}
