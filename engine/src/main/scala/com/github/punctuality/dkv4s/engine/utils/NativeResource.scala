package com.github.punctuality.dkv4s.engine.utils

import cats.effect.{Resource, Sync}
import org.rocksdb.AbstractNativeReference

object NativeResource {
  def nativeResource[F[_]: Sync, A <: AbstractNativeReference](fa: F[A]): Resource[F, A] =
    Resource.make(fa)(a => Sync[F].delay(a.close()))
}
