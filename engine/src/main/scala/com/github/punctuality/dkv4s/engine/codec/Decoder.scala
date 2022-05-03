package com.github.punctuality.dkv4s.engine.codec

import cats.Functor
import cats.syntax.functor._


trait Decoder[F[_], A] {
  def decode(bytes: Array[Byte]): F[Option[A]]
}

object Decoder {
  def apply[F[_], A](implicit ev: Decoder[F, A]): Decoder[F, A] = ev

  implicit def functor[F[_]: Functor]: Functor[Decoder[F, *]] = new Functor[Decoder[F, *]] {
    override def map[A, B](fa: Decoder[F, A])(f: A => B): Decoder[F, B] =
      (bytes: Array[Byte]) => fa.decode(bytes).map(_.map(f))
  }
}