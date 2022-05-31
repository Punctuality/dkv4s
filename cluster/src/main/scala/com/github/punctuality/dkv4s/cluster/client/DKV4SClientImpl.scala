package com.github.punctuality.dkv4s.cluster.client
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.semigroupal._
import cats.effect.{Async, Resource}
import cats.effect.syntax.spawn._
import cats.effect.std.Queue
import com.github.punctuality.dkv4s.cluster.multiraft.MultiRaft
import com.github.punctuality.dkv4s.cluster.rocks._
import com.github.punctuality.dkv4s.engine.RocksEngine
import com.github.punctuality.dkv4s.engine.codec._
import com.github.punctuality.dkv4s.raft.model._
import fs2._
import scodec.bits.ByteVector

class DKV4SClientImpl[F[+_]: Async](directEngine: RocksEngine[F],
                                    multiRaft: MultiRaft[F],
                                    changeQueue: Queue[F, WriteCommand[_]]
) extends DKV4SClient[F] {
  private def prepWrite[K: Encoder[F, *], V: Encoder[F, *]](key: K,
                                                            value: V
  ): F[(ByteVector, ByteVector)] =
    Encoder[F, K].encode(key).map(ByteVector(_)) product Encoder[F, V]
      .encode(value)
      .map(ByteVector(_))

  private def prepWriteMany[K: Encoder[F, *], V: Encoder[F, *]](
    values: List[(K, V)]
  ): F[List[(ByteVector, ByteVector)]] =
    values.traverse { case (key, value) =>
      Encoder[F, K].encode(key).map(ByteVector(_)) product Encoder[F, V]
        .encode(value)
        .map(ByteVector(_))
    }

  override def read[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] =
    directEngine.get[K, V](key)

  override def readMany[K: Encoder[F, *], V: Decoder[F, *]](keys: List[K]): F[List[Option[V]]] =
    directEngine.batchGet[K, V](keys)

  override def write[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit] =
    prepWrite(key, value).flatMap { case (bkey, bvalue) =>
      changeQueue.offer(WriteSingleCommand(bkey, bvalue))
    } >> directEngine.put(key, value)

  override def writeMany[K: Encoder[F, *], V: Encoder[F, *]](values: List[(K, V)]): F[Unit] =
    prepWriteMany(values)
      .flatMap(bvalues => changeQueue.offer(WriteManyCommand(bvalues))) >>
      directEngine.batchPut(values)

  override def delete[K: Encoder[F, *]](key: K): F[Unit] =
    Encoder[F, K]
      .encode(key)
      .flatMap(bkey => changeQueue.offer(DeleteSingleCommand(ByteVector(bkey)))) >>
      directEngine.delete(key)

  override def deleteMany[K: Encoder[F, *]](keys: List[K]): F[Unit] =
    keys
      .traverse(Encoder[F, K].encode(_).map(ByteVector(_)))
      .flatMap(bkeys => changeQueue.offer(DeleteManyCommand(bkeys))) >>
      keys.traverse(directEngine.delete[K]).void

  override def consistentRead[K: Encoder[F, *], V: Decoder[F, *]](key: K): F[Option[V]] =
    Encoder[F, K]
      .encode(key)
      .flatMap(bkey => multiRaft.execute(ReadSingleCommand(ByteVector(bkey))))
      .flatMap {
        case Some(value) => Decoder[F, V].decode(value.toArray)
        case None        => None.pure[F]
      }

  override def consistentReadMany[K: Encoder[F, *], V: Decoder[F, *]](
    keys: List[K]
  ): F[List[Option[V]]] =
    keys
      .traverse(Encoder[F, K].encode)
      .map(_.map(ByteVector(_)))
      .flatMap(bkeys => multiRaft.execute(ReadManyCommand(bkeys)))
      .flatMap(_.traverse {
        case Some(value) => Decoder[F, V].decode(value.toArray)
        case None        => None.pure[F]
      })

  override def consistentWrite[K: Encoder[F, *], V: Encoder[F, *]](key: K, value: V): F[Unit] =
    prepWrite(key, value).flatMap { case (bkey, bvalue) =>
      multiRaft.execute(WriteSingleCommand(bkey, bvalue))
    }

  override def consistentWriteMany[K: Encoder[F, *], V: Encoder[F, *]](
    values: List[(K, V)]
  ): F[Unit] =
    prepWriteMany(values).flatMap(bvalues => multiRaft.execute(WriteManyCommand(bvalues)))

  override def consistentDelete[K: Encoder[F, *]](key: K): F[Unit] =
    Encoder[F, K]
      .encode(key)
      .flatMap(bkey => multiRaft.execute(DeleteSingleCommand(ByteVector(bkey))))

  override def consistentDeleteMany[K: Encoder[F, *]](keys: List[K]): F[Unit] =
    keys
      .traverse(Encoder[F, K].encode(_).map(ByteVector(_)))
      .flatMap(bkeys => multiRaft.execute(DeleteManyCommand(bkeys)))
}

object DKV4SClientImpl {
  private val maxGroup = 1e+5.toInt

  def clientResource[F[+_]: Async](directEngine: RocksEngine[F],
                                   multiRaft: MultiRaft[F]
  ): Resource[F, DKV4SClientImpl[F]] =
    Resource
      .make(
        Queue
          .unbounded[F, WriteCommand[_]]
          .mproduct(queue =>
            Stream
              .fromQueueUnterminated(queue, maxGroup)
              .groupAdjacentByLimit(maxGroup)(_.getClass.getSimpleName)
              .evalMap {
                case (_, reads: Chunk[ReadSingleCommand]) =>
                  Async[F]
                    .delay(
                      ReadManyCommand(
                        reads
                          .foldLeft(List.newBuilder[ByteVector]) {
                            case (acc, ReadSingleCommand(key)) =>
                              acc.addOne(key)
                          }
                          .result()
                      )
                    )
                    .flatMap(multiRaft.execute)
                case (_, writes: Chunk[WriteSingleCommand]) =>
                  Async[F]
                    .delay(
                      WriteManyCommand(
                        writes
                          .foldLeft(List.newBuilder[(ByteVector, ByteVector)]) {
                            case (acc, WriteSingleCommand(key, value)) => acc.addOne(key -> value)
                          }
                          .result()
                      )
                    )
                    .flatMap(multiRaft.execute)
                case (_, deletes: Chunk[DeleteSingleCommand]) =>
                  Async[F]
                    .delay(
                      DeleteManyCommand(
                        deletes
                          .foldLeft(List.newBuilder[ByteVector]) {
                            case (acc, DeleteSingleCommand(key)) =>
                              acc.addOne(key)
                          }
                          .result()
                      )
                    )
                    .flatMap(multiRaft.execute)
                case (_, batchCommand) => batchCommand.traverse(multiRaft.execute(_)).void
              }
              .compile
              .drain
              .start
          )
      ) { case (_, fiber) =>
        fiber.cancel
      }
      .map { case (queue, _) =>
        new DKV4SClientImpl[F](directEngine, multiRaft, queue)
      }
}
