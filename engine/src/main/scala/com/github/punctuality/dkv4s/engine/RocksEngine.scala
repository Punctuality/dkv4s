package com.github.punctuality.dkv4s.engine

import cats.effect.{Resource, Sync}
import cats.syntax.applicative._
import cats.syntax.semigroupal._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.engine.codec._
import com.github.punctuality.dkv4s.engine.utils.NativeResource._
import org.rocksdb._

import scala.jdk.CollectionConverters._

final class RocksEngine[F[_]: Sync](underlying: RocksDB) {
  def get[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] = for {
    encodedK <- Encoder[F, K].encode(key)
    data     <- Sync[F].delay(underlying.get(encodedK))
    decodedV <- Decoder[F, V].decode(data)
  } yield decodedV

  def put[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit] =
    (Encoder[F, K].encode(key) product Encoder[F, V].encode(value)).flatMap {
      case (encodedK, encodedV) => Sync[F].delay(underlying.put(encodedK, encodedV))
    }

  def delete[K: Encoder[F, *]](key: K): F[Unit] =
    Encoder[F, K].encode(key).flatMap(encoded => Sync[F].delay(underlying.delete(encoded)))

  def isEmpty[K: Encoder[F, *]](key: K): F[Boolean] =
    Encoder[F, K]
      .encode(key)
      .flatMap(encoded =>
        Sync[F].delay(!underlying.keyMayExist(encoded, new Holder[Array[Byte]]()))
      )

  def poke[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] =
    for {
      encodedK <- Encoder[F, K].encode(key)
      holder    = new Holder(Array.emptyByteArray)
      canExist <- Sync[F].delay(underlying.keyMayExist(encodedK, holder))
      decodedV <- if (canExist) Decoder[F, V].decode(holder.getValue) else None.pure[F]
    } yield decodedV

  def batchPut[K: Encoder[F, *], V: Encoder[F, *]](entries: Seq[(K, V)]): F[Unit] =
    entries
      .traverse { case (key, value) =>
        Encoder[F, K].encode(key) product Encoder[F, V].encode(value)
      }
      .flatMap(data =>
        (nativeResource(Sync[F].delay(new WriteOptions())) product
          nativeResource(Sync[F].delay(new WriteBatch()))
            .evalMap(batch =>
              Sync[F]
                .delay(data.foreach { case (key, value) =>
                  batch.put(key, value)
                })
                .as(batch)
            ))
          .use { case (opts, batch) =>
            Sync[F].blocking(underlying.write(opts, batch))
          }
      )

  def flush(awaited: Boolean): F[Unit] =
    nativeResource(Sync[F].delay(new FlushOptions().setWaitForFlush(awaited)))
      .use(options => Sync[F].blocking(underlying.flush(options)))

  val backupResource: Resource[F, BackupEngine] = for {
    env    <- nativeResource(Sync[F].delay(Env.getDefault))
    opt    <- nativeResource(getProperty("path").map(new BackupEngineOptions(_)))
    engine <- nativeResource(Sync[F].delay(BackupEngine.open(env, opt)))
  } yield engine

  def backup(flushBeforeBackup: Boolean): F[Unit] =
    backupResource.use(be => Sync[F].blocking(be.createNewBackup(underlying, flushBeforeBackup)))
  def getBackups: F[List[BackupInfo]] =
    backupResource.use(be => Sync[F].blocking(be.getBackupInfo.asScala.toList))

  // TODO Not tested
  def restoreDB(backupId: Int): F[Unit] =
    Sync[F]
      .blocking(underlying.getLiveFiles().files.asScala)
      .map(_.head)
      .map(path => path.take(path.lastIndexOf('/')))
      .flatMap(dbDir =>
        backupResource.use(be =>
          Sync[F].blocking(be.restoreDbFromBackup(backupId, dbDir, dbDir, new RestoreOptions(true)))
        )
      )

  // TODO Maybe improve with Option[String]
  def getProperty(propName: String): F[String] =
    Sync[F].delay(underlying.getProperty(propName))
}

object RocksEngine {
  def apply[F[_]: Sync](underlying: RocksDB): RocksEngine[F] = new RocksEngine(underlying)

  def mkDB[F[_]: Sync](path: String, options: Options, ttl: Boolean = false): Resource[F, RocksDB] =
    Resource.eval(Sync[F].blocking(RocksDB.loadLibrary())) >>
      nativeResource(
        Sync[F].blocking(if (ttl) TtlDB.open(options, path) else RocksDB.open(options, path))
      )
}
