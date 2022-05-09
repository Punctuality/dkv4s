package test

import cats.effect.{IO, Ref}
import com.github.punctuality.raft.model.{ReadCommand, WriteCommand}
import com.github.punctuality.raft.storage.StateMachine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

case class SetCommand(key: String, value: String) extends WriteCommand[String]
case class DeleteCommand(key: String)             extends WriteCommand[Unit]
case class GetCommand(key: String)                extends ReadCommand[String]

class KvStateMachine(lastIndex: Ref[IO, Long], map: Ref[IO, Map[String, String]])
  extends StateMachine[IO] {

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = {
    case (index, SetCommand(key, value)) =>
      for {
        _ <- map.update(_ + (key -> value))
        _ <- lastIndex.set(index)
      } yield value

    case (index, DeleteCommand(key)) =>
      for {
        _ <- map.update(_.removed(key))
        _ <- lastIndex.set(index)
      } yield ()
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = { case GetCommand(key) =>
    for {
      items <- map.get
      _      = println(items)
    } yield items(key)
  }

  override def appliedIndex: IO[Long] = lastIndex.get

  override def takeSnapshot: IO[(Long, ByteBuffer)] =
    for {
      items <- map.get
      index <- lastIndex.get
      bytes  = serialize(items)
    } yield (index, bytes)

  override def restoreSnapshot(index: Long, bytes: ByteBuffer): IO[Unit] =
    for {
      _ <- map.set(deserialize(bytes))
      _ <- lastIndex.set(index)
    } yield ()

  private def serialize(items: Map[String, String]): ByteBuffer = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(items)
    oos.close()

    ByteBuffer.wrap(stream.toByteArray)
  }

  private def deserialize(bytes: ByteBuffer): Map[String, String] = {

    val ois      = new ObjectInputStream(new ByteArrayInputStream(bytes.array()))
    val response = ois.readObject().asInstanceOf[Map[String, String]]
    ois.close()

    response
  }

}

object KvStateMachine {
  def empty: IO[KvStateMachine] =
    for {
      index <- Ref.of[IO, Long](0L)
      map   <- Ref.of[IO, Map[String, String]](Map.empty)
    } yield new KvStateMachine(index, map)
}
