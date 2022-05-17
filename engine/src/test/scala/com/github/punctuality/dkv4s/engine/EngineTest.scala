package com.github.punctuality.dkv4s.engine

//import cats.effect.{ExitCode, IO, IOApp}
//import com.github.punctuality.dkv4s.engine.utils.NativeResource.nativeResource
//import org.rocksdb.Options

//object EngineTest extends IOApp {
//  override def run(args: List[String]): IO[ExitCode] =
//    nativeResource(IO(new Options().setCreateIfMissing(true)))
//      .flatMap(options =>
//        RocksEngine
//          .initDB[IO]("tmp/rocks", options, ttl = false)
//          .evalMap(RocksEngine[IO])
//      )
//      .use(engine =>
//        engine.get[String, String]("key1").map(r => println(s"Key1 = $r")) >>
//          engine.get[String, String]("key2").map(r => println(s"Key2 = $r")) >>
//          engine.put[String, String]("key1", "value1").map(_ => println(s"Key1 Set")) >>
//          engine.put[String, String]("key2", "value2").map(_ => println(s"Key2 Set")) >>
//          engine.get[String, String]("key1").map(r => println(s"Key1 = $r")) >>
//          engine.get[String, String]("key2").map(r => println(s"Key2 = $r")) >>
//          engine.delete("key1").map(_ => println(s"Key1 Delete")) >>
//          engine.delete("key2").map(_ => println(s"Key1 Delete")) >>
//          engine.get[String, String]("key1").map(r => println(s"Key1 = $r")) >>
//          engine.get[String, String]("key2").map(r => println(s"Key2 = $r"))
//      )
//      .as(ExitCode.Success)
//
//}
