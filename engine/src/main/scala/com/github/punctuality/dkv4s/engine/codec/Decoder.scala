package com.github.punctuality.dkv4s.engine.codec

import cats.Functor
import cats.effect.Sync
import cats.syntax.functor._
import scodec.Codec
import scodec.bits.BitVector

import java.nio.charset.StandardCharsets

trait Decoder[F[_], A] {
  def decode(bytes: Array[Byte]): F[Option[A]]
}

object Decoder {
  def apply[F[_], A](implicit ev: Decoder[F, A]): Decoder[F, A] = ev

  implicit def functor[F[_]: Functor]: Functor[Decoder[F, *]] = new Functor[Decoder[F, *]] {
    override def map[A, B](fa: Decoder[F, A])(f: A => B): Decoder[F, B] =
      (bytes: Array[Byte]) => fa.decode(bytes).map(_.map(f))
  }

  implicit def stringDec[F[_]: Sync]: Decoder[F, String] =
    (bytes: Array[Byte]) => Sync[F].delay(Option(bytes).map(new String(_, StandardCharsets.UTF_8)))

  implicit def scodecDec[F[_]: Sync, N: Codec]: Decoder[F, N] = (data: Array[Byte]) =>
    Sync[F].delay(Codec[N].decode(BitVector(data)).map(_.value).toOption)
}
