package com.github.punctuality.dkv4s.engine.codec

import cats.Contravariant
import cats.effect.kernel.Sync
import scodec.Codec

import java.nio.charset.StandardCharsets

trait Encoder[F[_], A] {
  def encode(data: A): F[Array[Byte]]
}

object Encoder {
  def apply[F[_], A](implicit ev: Encoder[F, A]): Encoder[F, A] = ev

  implicit def contravariant[F[_]]: Contravariant[Encoder[F, *]] =
    new Contravariant[Encoder[F, *]] {
      override def contramap[A, B](fa: Encoder[F, A])(f: B => A): Encoder[F, B] = (data: B) =>
        fa.encode(f(data))
    }

  implicit def stringEnc[F[_]: Sync]: Encoder[F, String] =
    (data: String) => Sync[F].delay(data.getBytes(StandardCharsets.UTF_8))

  implicit def scodecEnc[F[_]: Sync, N: Codec]: Encoder[F, N] =
    (data: N) =>
      Sync[F].defer(
        Codec[N]
          .encode(data)
          .fold(
            err => Sync[F].raiseError(new RuntimeException(err.messageWithContext)),
            r => Sync[F].pure(r.toByteArray)
          )
      )
}
