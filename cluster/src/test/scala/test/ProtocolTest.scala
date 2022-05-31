package test

import cats.effect.{ExitCode, IO, IOApp}
import com.github.punctuality.dkv4s.cluster.network.Protocol._
import scodec._
import scodec.codecs.implicits.implicitByteVectorCodec
import scodec.bits.ByteVector

import java.nio.charset.{Charset, StandardCharsets}

object ProtocolTest extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = IO {
    implicit val utf8: Charset = StandardCharsets.UTF_8

    val putC = Put(
      ByteVector.encodeString("KeyKeyKeyKeyKey").toOption.get,
      ByteVector.encodeString("ValueValueValueValueValueValue").toOption.get
    )

    val encoded = Codec[StorageCommand].encode(putC)
    val decoded = Codec[StorageCommand].decode(encoded.toOption.get)
    println(s"Initial: $putC")
    println(s"Initial: $encoded")
    println(s"Decoded: $decoded")

    ExitCode.Success
  }
}
