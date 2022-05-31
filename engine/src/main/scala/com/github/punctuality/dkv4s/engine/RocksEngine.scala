package com.github.punctuality.dkv4s.engine

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.syntax.applicative._
import cats.syntax.semigroupal._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.engine.codec._
import com.github.punctuality.dkv4s.engine.utils.NativeResource._
import org.rocksdb._
import scodec.codecs.implicits._

import scala.jdk.CollectionConverters._

final class RocksEngine[F[_]: Async](val dbRef: Ref[F, (RocksDB, F[Unit])],
                                     stopWorld: Ref[F, Deferred[F, Unit]]
) {
  private def withPermission[T](f: RocksDB => F[T]): F[T] =
    dbRef.get.flatMap(rdb => stopWorld.get.flatMap(_.get) >> f(rdb._1))
  private val revokePermission: Resource[F, RocksDB] =
    Resource
      .make(Deferred[F, Unit].flatMap(stopWorld.set).void)(_ => stopWorld.get.map(_.complete(())))
      .evalMap(_ => dbRef.get.map(_._1))

  def get[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] = for {
    encodedK <- Encoder[F, K].encode(key)
    data     <- withPermission(db => Sync[F].delay(db.get(encodedK)))
    decodedV <- Decoder[F, V].decode(data)
  } yield decodedV

  def put[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit] =
    (Encoder[F, K].encode(key) product Encoder[F, V].encode(value)).flatMap {
      case (encodedK, encodedV) => withPermission(db => Sync[F].delay(db.put(encodedK, encodedV)))
    }

  def delete[K: Encoder[F, *]](key: K): F[Unit] =
    Encoder[F, K]
      .encode(key)
      .flatMap(encoded => withPermission(db => Sync[F].delay(db.delete(encoded))))

  def isEmpty[K: Encoder[F, *]](key: K): F[Boolean] =
    Encoder[F, K]
      .encode(key)
      .flatMap(encoded =>
        withPermission(db => Sync[F].delay(!db.keyMayExist(encoded, new Holder[Array[Byte]]())))
      )

  def poke[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] =
    for {
      encodedK <- Encoder[F, K].encode(key)
      holder    = new Holder(Array.emptyByteArray)
      canExist <- withPermission(db => Sync[F].delay(db.keyMayExist(encodedK, holder)))
      decodedV <- if (canExist) Decoder[F, V].decode(holder.getValue) else None.pure[F]
    } yield decodedV

  def batchGet[K: Encoder[F, *], V: Decoder[F, *]](keys: List[K]): F[List[Option[V]]] = for {
    encodedKeys   <- keys.traverse(Encoder[F, K].encode)
    data          <- withPermission(db => Sync[F].blocking(db.multiGetAsList(encodedKeys.asJava)))
    dataS         <- Sync[F].delay(data.asScala.toList)
    decodedValues <- dataS.traverse(Decoder[F, V].decode)
  } yield decodedValues

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
            withPermission(db => Sync[F].blocking(db.write(opts, batch)))
          }
      )

  def flush(awaited: Boolean): F[Unit] =
    nativeResource(Sync[F].delay(new FlushOptions().setWaitForFlush(awaited)))
      .use(options => withPermission(db => Sync[F].blocking(db.flush(options))))

  val backupResource: Resource[F, BackupEngine] = for {
    env    <- nativeResource(Sync[F].delay(Env.getDefault))
    opt    <- nativeResource(getProperty("path").map(new BackupEngineOptions(_)))
    engine <- nativeResource(Sync[F].delay(BackupEngine.open(env, opt)))
  } yield engine

  def backup(flushBeforeBackup: Boolean): F[Unit] =
    (for {
      db <- revokePermission
      be <- backupResource
      _  <- Resource.eval(Sync[F].blocking(be.createNewBackup(db, flushBeforeBackup)))
    } yield ()).use_

  def getBackups: F[List[BackupInfo]] =
    backupResource.use(be => Sync[F].blocking(be.getBackupInfo.asScala.toList))

  // TODO Not tested
  def restoreDB(backupId: Int): F[Unit] =
    revokePermission.use(db =>
      Sync[F]
        .blocking(db.getLiveFiles().files.asScala)
        .map(_.head)
        .map(path => path.take(path.lastIndexOf('/')))
        .flatMap(dbDir =>
          backupResource.use(be =>
            Sync[F].blocking {
              be.restoreDbFromBackup(backupId, dbDir, dbDir, new RestoreOptions(true))
              be.garbageCollect()
            }
          )
        )
    )

  def getProperty(propName: String): F[String] =
    withPermission(db => Sync[F].delay(db.getProperty(propName)))

  def compact: F[Unit] =
    withPermission(db => Sync[F].blocking(db.compactRange()))

  def checkpoint(path: String): Resource[F, Unit] =
    revokePermission.flatMap(db =>
      nativeResource(Sync[F].delay(Checkpoint.create(db))).evalMap(cp =>
        Sync[F].blocking(cp.createCheckpoint(path))
      )
    )

  def dbGenStop: Resource[F, Unit] = revokePermission.flatMap(db =>
    Resource.make(Sync[F].blocking {
      db.pauseBackgroundWork()
      db.disableFileDeletions()
    })(_ =>
      Sync[F].blocking {
        db.continueBackgroundWork()
        db.enableFileDeletions(true)
      }
    )
  )

  def hotSwap(newDB: Resource[F, RocksDB]): F[Unit] =
    newDB.allocated.flatMap(dbRef.getAndSet(_).flatMap(_._2))

  def close: F[Unit] =
    dbRef.get.flatMap(_._2)
}

object RocksEngine {
  def apply[F[_]: Async](underlying: Resource[F, RocksDB]): F[RocksEngine[F]] =
    for {
      completedDeferred <- Deferred[F, Unit]
      _                 <- completedDeferred.complete(())
      ref               <- Ref.of(completedDeferred)
      dbRef             <- Ref.ofEffect(underlying.allocated)
    } yield new RocksEngine(dbRef, ref)

  def openDB[F[_]: Sync](path: String,
                         options: Options,
                         ttl: Boolean = false
  ): Resource[F, RocksDB] =
    nativeResource(
      Sync[F].blocking(if (ttl) TtlDB.open(options, path) else RocksDB.open(options, path))
    )

  def initDB[F[_]: Sync](path: String,
                         options: Options,
                         ttl: Boolean = false
  ): Resource[F, RocksDB] =
    Resource.eval(Sync[F].blocking(RocksDB.loadLibrary())) >> openDB(path, options, ttl)

}
