package com.github.punctuality.dkv4s.cluster.bench

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.syntax.traverse._
import com.aerospike.client._
import com.aerospike.client.policy.{BatchPolicy, ClientPolicy, WritePolicy}
import com.github.punctuality.dkv4s.cluster.util.Emojis
import com.github.punctuality.dkv4s.engine.RocksEngine
import com.github.punctuality.dkv4s.engine.utils.NativeResource.nativeResource
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.{Redis, RedisCommands}
import dev.profunktor.redis4cats.effect.Log.NoOp._
import fs2.io.file.Files
import org.rocksdb.Options
import org.scalameter.api._
import org.scalameter.{Bench, KeyValue}
import scodec.bits.ByteVector
import scodec.codecs.implicits._

import scala.util.Random

object SingleStorageBench extends Bench.LocalTime {
  trait RocksDBEnv {
    val db: RocksEngine[IO] = RocksEngine[IO](for {
      tmpDir <- Resource.make(Files[IO].createTempDirectory(None, "rocks", None))(
                  Files[IO].deleteRecursively
                )
      _           = println(s"Creating RocksDB at $tmpDir")
      options    <- nativeResource(IO.delay(new Options().setCreateIfMissing(true)))
      underlying <- RocksEngine.initDB[IO](tmpDir.toString, options)
    } yield underlying).unsafeRunSync()
  }

  trait RedisDBEnv {
    val db: Resource[IO, RedisCommands[IO, String, String]] =
      Redis[IO].simple("redis://localhost:6666", RedisCodec.Utf8)
  }

  trait AerospikeDBEnv {
    val cp = new ClientPolicy()
    val db = new AerospikeClient(cp, "localhost", 3000)
  }

  private val curRandom = new Random(1)
  private def genRocksData(size: Int) =
    List.fill(size)(ByteVector(curRandom.nextBytes(10)) -> ByteVector(curRandom.nextBytes(10)))
  private def genRedisData(size: Int) =
    List.fill(size)(
      Iterator.fill(10)(Emojis.randomEmoji).mkString("") -> Iterator
        .fill(10)(Emojis.randomEmoji)
        .mkString("")
    )
  private def genAerospikeData(size: Int) =
    List.fill(size)(
      Iterator.fill(10)(Emojis.randomEmoji).mkString("") -> Iterator
        .fill(10)(Emojis.randomEmoji)
        .mkString("")
    )

  val context = Context(
    KeyValue(exec.benchRuns     -> 2),
    KeyValue(exec.minWarmupRuns -> 1),
    KeyValue(exec.maxWarmupRuns -> 5)
  )

  val redisContext = Context(
    KeyValue(exec.benchRuns     -> 1),
    KeyValue(exec.minWarmupRuns -> 1),
    KeyValue(exec.maxWarmupRuns -> 1)
  )

  val aerospikeContext = Context(
    KeyValue(exec.benchRuns     -> 1),
    KeyValue(exec.minWarmupRuns -> 1),
    KeyValue(exec.maxWarmupRuns -> 1)
  )

  performance of "RocksDB" in new RocksDBEnv {
    val batches: Gen[(Int, List[(ByteVector, ByteVector)])] =
      Gen
        .range("batch_size")(1_000, 101_000, 10_000)
        .map(size => size -> genRocksData(size))
        .cached

    measure method "put" in {
      using(batches) config context in { case (_, values) =>
        values.traverse { case (k, v) => db.put(k, v) }.unsafeRunSync()
      }
    }

    measure method "parallel put" in {
      using(batches) config context in { case (_, values) =>
        IO.parTraverseN(5)(values)({ case (k, v) => db.put(k, v) }).unsafeRunSync()
      }
    }

    measure method "batchPut" in {
      using(batches) config context in { case (_, values) =>
        db.batchPut(values).unsafeRunSync()
      }
    }

    measure method "get" in {
      using(batches) config context in { case (_, values) =>
        values.traverse(kv => db.get(kv._1)).unsafeRunSync()
      }
    }

    measure method "parallel get" in {
      using(batches) config context in { case (_, values) =>
        IO.parTraverseN(5)(values)(kv => db.get(kv._1)).unsafeRunSync()
      }
    }

    measure method "batch get" in {
      using(batches) config context in { case (_, values) =>
        db.batchGet(values.map(_._1)).unsafeRunSync()
      }
    }
  }

  performance of "Redis" in new RedisDBEnv {
    val batches: Gen[(Int, List[(String, String)])] =
      Gen
        .range("batch_size")(1_000, 101_000, 10_000)
        .map(size => size -> genRedisData(size))
        .cached

    measure method "put" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis Put: $size")
        db.use(client => values.traverse { case (k, v) => client.set(k, v) }).unsafeRunSync()
      }
    }

    measure method "parallel put" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis Par Put: $size")
        db.use(client => IO.parTraverseN(5)(values)({ case (k, v) => client.set(k, v) }))
          .unsafeRunSync()
      }
    }

    measure method "batchPut" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis batch Put: $size")
        db.use(client => client.mSet(values.toMap)).unsafeRunSync()
      }
    }

    measure method "get" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis Get: $size")
        db.use(client => values.traverse(kv => client.get(kv._1))).unsafeRunSync()
      }
    }

    measure method "parallel get" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis Par Get: $size")
        db.use(client => IO.parTraverseN(5)(values)(kv => client.get(kv._1))).unsafeRunSync()

      }
    }

    measure method "batch get" in {
      using(batches) config redisContext in { case (size, values) =>
        println(s"Redis batch Get: $size")
        db.use(client => client.mGet(values.map(_._1).toSet)).unsafeRunSync()
      }
    }
  }

  performance of "Aerospike" in new AerospikeDBEnv {
    val batches: Gen[(Int, List[(String, String)])] =
      Gen
        .range("batch_size")(1_000, 101_000, 10_000)
        .map(size => size -> genAerospikeData(size))
        .cached

    val wp = new WritePolicy()
    val bp = new BatchPolicy()
    bp.respondAllKeys = true

    measure method "put" in {
      using(batches) config aerospikeContext in { case (size, values) =>
        println(s"Aerospike Put: $size")
        values
          .traverse { case (k, v) =>
            IO.delay(db.put(wp, new Key("test", "test", k), new Bin("value", v)))
          }
          .unsafeRunSync()
      }
    }

    measure method "parallel put" in {
      using(batches) config aerospikeContext in { case (size, values) =>
        println(s"Aerospike Par Put: $size")
        IO.parTraverseN(5)(values) { case (k, v) =>
          IO.delay(db.put(wp, new Key("test", "test", k), new Bin("value", v)))
        }.unsafeRunSync()
      }
    }

    measure method "get" in {
      using(batches) config aerospikeContext in { case (size, values) =>
        println(s"Aerospike Get: $size")
        values
          .traverse(kv => IO.delay(db.get(bp, new Key("test", "test", kv._1), "value")))
          .unsafeRunSync()
      }
    }

    measure method "parallel get" in {
      using(batches) config aerospikeContext in { case (size, values) =>
        println(s"Aerospike Par Get: $size")
        IO.parTraverseN(5)(values)(kv =>
          IO.delay(db.get(bp, new Key("test", "test", kv._1), "value"))
        ).unsafeRunSync()
      }
    }

    measure method "batch get" in {
      using(batches) config aerospikeContext in { case (size, values) =>
        println(s"Aerospike batch Get: $size")
        fs2.Stream
          .emits(values)
          .chunkN(1000, allowFewer = true)
          .evalMap(chunk =>
            IO.delay(db.get(bp, chunk.map(kv => new Key("test", "test", kv._1)).toArray, "value"))
          )
          .compile
          .drain
          .unsafeRunSync()
      }
    }
  }
}
