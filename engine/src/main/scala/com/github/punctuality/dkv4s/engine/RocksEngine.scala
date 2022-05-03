package com.github.punctuality.dkv4s.engine

import cats.effect.{Resource, Sync}
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.engine.codec._
import com.github.punctuality.dkv4s.engine.utils.NativeResource._
import org.rocksdb._

import scala.jdk.CollectionConverters._

final class RocksEngine[F[_]: Sync](underlying: RocksDB) {
  def get[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] = for {
    encodedK <- Encoder[F, K].encode(key)
    data <- Sync[F].delay(underlying.get(encodedK))
    decodedV <- Decoder[F, V].decode(data)
  } yield decodedV

  def put[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit] = for {
    encodedK <- Encoder[F, K].encode(key)
    encodedV <- Encoder[F, V].encode(value)
    _ <- Sync[F].delay(underlying.put(encodedK, encodedV))
  } yield ()

  def delete[K: Encoder[F, *]](key: K): F[Unit] =
    Encoder[F, K].encode(key).flatMap(encoded => Sync[F].delay(underlying.delete(encoded)))

  def isEmpty[K: Encoder[F, *]](key: K): F[Boolean] =
    Encoder[F, K].encode(key).flatMap(encoded =>
      Sync[F].delay(!underlying.keyMayExist(encoded, new Holder[Array[Byte]]()))
    )

  def poke[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] = {
    for {
      encodedK <- Encoder[F, K].encode(key)
      holder = new Holder(Array.emptyByteArray)
      canExist <- Sync[F].delay(underlying.keyMayExist(encodedK, holder))
      decodedV <- if (canExist) Decoder[F, V].decode(holder.getValue) else None.pure[F]
    } yield decodedV
  }

  def flush(awaited: Boolean): F[Unit] =
    nativeResource(Sync[F].delay(new FlushOptions().setWaitForFlush(awaited))).use(options =>
      Sync[F].delay(underlying.flush(options))
    )

  val backupResource: Resource[F, BackupEngine] = for {
    env <- nativeResource(Sync[F].delay(Env.getDefault))
    opt <- nativeResource(getProperty("path").map(new BackupEngineOptions(_)))
    engine <- nativeResource(Sync[F].delay(BackupEngine.open(env, opt)))
  } yield engine

  def backup(flushBeforeBackup: Boolean): F[Unit] =
    backupResource.use(be => Sync[F].delay(be.createNewBackup(underlying, flushBeforeBackup)))
  def getBackups: F[List[BackupInfo]] =
    backupResource.use(be => Sync[F].delay(be.getBackupInfo.asScala))

  // TODO Not tested
  def restoreDB(backupId: Int): F[Unit] = for {
    dbDir <- Sync[F].delay(underlying.getLiveFiles().files.asScala)
      .map(_.head)
      .map(path => path.take(path.lastIndexOf('/')))
    _ <- backupResource.use(be =>
      Sync[F].delay( be.restoreDbFromBackup(backupId, dbDir, dbDir, new RestoreOptions(true)))
    )
  } yield ()

  // TODO Maybe improve with Option[String]
  def getProperty(propName: String): F[String] =
    Sync[F].delay(underlying.getProperty(propName))
}

object RocksEngine {
  def apply[F[_] : Sync](underlying: RocksDB): RocksEngine[F] = new RocksEngine(underlying)

  // TODO Add easier constructors
}