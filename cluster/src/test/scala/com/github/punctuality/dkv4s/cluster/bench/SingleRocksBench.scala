package com.github.punctuality.dkv4s.cluster.bench

import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.traverse._
import com.github.punctuality.dkv4s.engine.RocksEngine
import com.github.punctuality.dkv4s.engine.utils.NativeResource.nativeResource
import fs2.io.file.Files
import org.rocksdb.Options
import org.scalameter.{Bench, KeyValue}
import org.scalameter.api._
import scodec.bits.ByteVector
import scodec.codecs.implicits._

import scala.util.Random

object SingleRocksBench extends Bench.LocalTime {
//  def persistor = new SerializationPersistor

  trait TestEnv {
    val db: RocksEngine[IO] = RocksEngine[IO](for {
      tmpDir <- Resource.make(Files[IO].createTempDirectory(None, "rocks", None))(
                  Files[IO].deleteRecursively
                )
      _           = println(s"Creating RocksDB at $tmpDir")
      options    <- nativeResource(IO.delay(new Options().setCreateIfMissing(true)))
      underlying <- RocksEngine.initDB[IO](tmpDir.toString, options)
    } yield underlying).unsafeRunSync()
  }

  private val curRandom = new Random(1)
  private def genData(size: Int) =
    List.fill(size)(ByteVector(curRandom.nextBytes(3)) -> ByteVector(curRandom.nextBytes(10)))

  performance of "RocksDB" in new TestEnv {
    val batches: Gen[(Int, List[(ByteVector, ByteVector)])] =
      Gen.range("batch_size")(1_000, 101_000, 10_000).map(size => size -> genData(size)).cached

    measure method "put" in {
      using(batches) config (
        KeyValue(exec.benchRuns     -> 2),
        KeyValue(exec.minWarmupRuns -> 1),
        KeyValue(exec.maxWarmupRuns -> 5),
      ) in { case (_, values) =>
        values.traverse { case (k, v) => db.put(k, v) }.unsafeRunSync()
      }
    }

    measure method "parallel put" in {
      using(batches) config (
        KeyValue(exec.benchRuns     -> 2),
        KeyValue(exec.minWarmupRuns -> 1),
        KeyValue(exec.maxWarmupRuns -> 5),
      ) in { case (_, values) =>
        IO.parTraverseN(5)(values)({ case (k, v) => db.put(k, v) }).unsafeRunSync()
      }
    }

    measure method "batchPut" in {
      using(batches) config (
        KeyValue(exec.benchRuns     -> 2),
        KeyValue(exec.minWarmupRuns -> 1),
        KeyValue(exec.maxWarmupRuns -> 5),
      ) in { case (_, values) =>
        db.batchPut(values).unsafeRunSync()
      }
    }

    measure method "get" in {
      using(batches) config (
        KeyValue(exec.benchRuns     -> 2),
        KeyValue(exec.minWarmupRuns -> 1),
        KeyValue(exec.maxWarmupRuns -> 5),
      ) in { case (_, values) =>
        values.traverse(kv => db.get(kv._1)).unsafeRunSync()
      }
    }

    measure method "batch get" in {
      using(batches) config (
        KeyValue(exec.benchRuns     -> 2),
        KeyValue(exec.minWarmupRuns -> 1),
        KeyValue(exec.maxWarmupRuns -> 5),
      ) in { case (_, values) =>
        db.batchGet(values.map(_._1)).unsafeRunSync()
      }
    }
  }
}
