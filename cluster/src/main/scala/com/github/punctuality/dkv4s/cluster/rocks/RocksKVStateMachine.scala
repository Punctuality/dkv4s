package com.github.punctuality.dkv4s.cluster.rocks

import cats.effect.{Async, Resource}
import cats.syntax.traverse._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.punctuality.dkv4s.engine.RocksEngine
import com.github.punctuality.dkv4s.engine.codec._
import com.github.punctuality.dkv4s.raft.model.{ReadCommand, WriteCommand}
import com.github.punctuality.dkv4s.raft.storage.StateMachine
import com.github.punctuality.dkv4s.cluster.util.Zipping
import com.github.punctuality.dkv4s.engine.utils.NativeResource._
import fs2.io.file.{Files, Path => FPath}
import org.rocksdb.Options
import scodec.bits.ByteVector
import scodec.codecs.implicits._
import scodec.codecs.implicits._

import java.nio.ByteBuffer

class RocksKVStateMachine[F[_]: Async](engine: RocksEngine[F]) extends StateMachine[F] {
  import RocksKVStateMachine._

  implicit def byteVecEncoder: Encoder[F, ByteVector] = (data: ByteVector) =>
    Async[F].delay(data.toArray)
  implicit def byteVecDecoder: Decoder[F, ByteVector] = (data: Array[Byte]) =>
    Async[F].delay(Option(ByteVector(data)))

  override def applyWrite: WriteHandler = {
    case (index, WriteSingleCommand(key, value)) => engine.put(key, value) >> setIndex(index)
    case (index, WriteManyCommand(many))         => engine.batchPut(many) >> setIndex(index)
    case (index, DeleteSingleCommand(key))       => engine.delete(key) >> setIndex(index)
    case (index, DeleteManyCommand(keys)) =>
      keys.traverse(engine.delete[ByteVector]) >> setIndex(index)
  }: WriteHandler

  override def applyRead: ReadHandler = {
    case ReadSingleCommand(key) => engine.get[ByteVector, ByteVector](key)
    case ReadManyCommand(keys)  => engine.batchGet[ByteVector, ByteVector](keys)
  }: ReadHandler

  override def appliedIndex: F[Long] =
    engine.get[String, Long](appliedIndexKey).map(_.getOrElse(0))

  private def setIndex(index: Long): F[Unit] =
    engine.put(appliedIndexKey, index)

  override def takeSnapshot: F[(Long, ByteBuffer)] =
    (for {
      _      <- engine.dbGenStop
      tmpDir <- Resource.make(Files[F].createTempDirectory)(Files[F].deleteRecursively)
      _      <- engine.checkpoint(tmpDir.toString)
      tmpZip <- Resource.make(Files[F].createTempFile)(Files[F].delete)
      _      <- Resource eval Zipping.convertToZip(tmpDir.toNioPath.toFile, tmpZip.toNioPath.toFile)
    } yield tmpZip).use(zip =>
      for {
        zipSize <- Files[F].size(zip).map(_.toInt)
        formedBB <-
          Files[F]
            .readAll(zip)
            .compile
            .foldChunks(ByteBuffer.allocate(zipSize))((bb, chunk) => bb.put(chunk.toByteBuffer))
        index <- appliedIndex
      } yield index -> formedBB
    )

  override def restoreSnapshot(index: Long, bytes: ByteBuffer): F[Unit] =
    (for {
      _      <- engine.dbGenStop
      tmpZip <- Resource.make(Files[F].createTempFile)(Files[F].delete)
      _ <- Resource eval fs2.Stream
             .chunk(fs2.Chunk.ByteBuffer(bytes))
             .chunkLimit(1 << 16)
             .flatMap(fs2.Stream.chunk)
             .through(Files[F].writeAll(tmpZip))
             .compile
             .drain
      tmpDir   = FPath(s"tmp/rocksKV${System.currentTimeMillis()}")
      _       <- Resource.eval(Files[F].createDirectory(tmpDir))
      _       <- Resource eval Zipping.convertFromZip(tmpZip.toNioPath.toFile, tmpDir.toNioPath.toFile)
      options <- nativeResource(Async[F].delay(new Options().setCreateIfMissing(false)))
    } yield tmpDir -> options).use { case (dir, opts) =>
      engine.hotSwap(RocksEngine.openDB(dir.toString, opts, ttl = false))
    }
}

object RocksKVStateMachine {
  val appliedIndexKey: String = "__applied_index__"

  def apply[F[_]: Async](engine: RocksEngine[F]): RocksKVStateMachine[F] =
    new RocksKVStateMachine(engine)
}
