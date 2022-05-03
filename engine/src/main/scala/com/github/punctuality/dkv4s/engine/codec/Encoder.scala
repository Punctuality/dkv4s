package com.github.punctuality.dkv4s.engine.codec

import cats.Contravariant

trait Encoder[F[_], A] {
  def encode(data: A): F[Array[Byte]]
}

object Encoder {
  def apply[F[_], A](implicit ev: Encoder[F, A]): Encoder[F, A] = ev

  implicit def contravariant[F[_]]: Contravariant[Encoder[F, *]] =
    new Contravariant[Encoder[F, *]] {
      override def contramap[A, B](fa: Encoder[F, A])(f: B => A): Encoder[F, B] = (data: B) => fa.encode(f(data))
    }
}