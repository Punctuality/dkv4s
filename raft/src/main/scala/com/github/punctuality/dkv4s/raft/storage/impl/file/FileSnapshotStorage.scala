package com.github.punctuality.dkv4s.raft.storage.impl.file

import cats.effect.{Ref, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.punctuality.dkv4s.raft.model.{Node, Snapshot}
import com.github.punctuality.dkv4s.raft.protocol.{ClusterConfiguration, JointClusterConfiguration, NewClusterConfiguration}
import com.github.punctuality.dkv4s.raft.service.ErrorLogging
import com.github.punctuality.dkv4s.raft.storage.SnapshotStorage
import com.github.punctuality.dkv4s.raft.util.Logger

import java.nio
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

class FileSnapshotStorage[F[_]: Sync: Logger](path: Path, latestSnapshot: Ref[F, Option[Snapshot]])
  extends SnapshotStorage[F] with ErrorLogging[F] {

  override def getLatestSnapshot: F[Option[Snapshot]] =
    latestSnapshot.get

  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    errorLogging(s"Saving Snapshot in file system $path") {
      Sync[F].delay(tryStoreInFileSystem(snapshot)) >> latestSnapshot.set(Some(snapshot))
    }

  override def retrieveSnapshot: F[Option[Snapshot]] =
    errorLogging(s"Retrieving Snapshot from file system $path") {
      Sync[F].delay(tryLoadFromFileSystem().toOption).flatTap(latestSnapshot.set)
    }

  protected def tryLoadFromFileSystem(): Try[Snapshot] =
    for {
      lines <- Try(
                 Files.readAllLines(path.resolve("snapshot_state"), StandardCharsets.UTF_8).asScala
               )
      lastIndex  <- Try(lines.head.toLong)
      bytebuffer <- Try(Files.readAllBytes(path.resolve("snapshot"))).map(nio.ByteBuffer.wrap)
      config <- Try(Files.readAllLines(path.resolve("snapshot_config")))
                  .map(_.asScala.toList)
                  .map(decodeConfig)
    } yield Snapshot(lastIndex, bytebuffer, config)

  protected def tryStoreInFileSystem(snapshot: Snapshot): Try[Path] = Try {
    Files.write(
      path.resolve("snapshot_config"),
      encodeConfig(snapshot.config).asJava,
      StandardCharsets.UTF_8
    )
    Files.write(
      path.resolve("snapshot_state"),
      List(snapshot.lastIndex.toString).asJava,
      StandardCharsets.UTF_8
    )
    Files.write(path.resolve("snapshot"), snapshot.bytes.array())
  }

  protected def encodeConfig(config: ClusterConfiguration): List[String] =
    config match {
      case JointClusterConfiguration(oldMembers, newMembers) =>
        List(oldMembers.map(_.id).mkString(","), newMembers.map(_.id).mkString(","))
      case NewClusterConfiguration(members) =>
        List(members.map(_.id).mkString(","))
    }

  protected def decodeConfig(lines: List[String]): ClusterConfiguration =
    lines match {
      case first :: Nil =>
        NewClusterConfiguration(
          first.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet
        )
      case first :: second :: Nil =>
        JointClusterConfiguration(
          first.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet,
          second.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet
        )
      case _ =>
        NewClusterConfiguration(Set.empty)
    }
}

object FileSnapshotStorage {
  def open[F[_]: Sync: Logger](path: Path): F[FileSnapshotStorage[F]] =
    Ref.of[F, Option[Snapshot]](None).map(new FileSnapshotStorage[F](path, _))
}
