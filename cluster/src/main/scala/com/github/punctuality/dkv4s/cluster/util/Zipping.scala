package com.github.punctuality.dkv4s.cluster.util

import cats.effect.Async
import cats.effect.{Resource, Sync}
import cats.syntax.traverse._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.io.file.{Files, Path => FPath}
import sun.awt.shell.ShellFolder

import java.io.{File, FileInputStream, FileOutputStream, IOException, InputStream}
import java.util.jar.JarEntry
import java.util.zip._

object Zipping {

  def convertToZip[F[_]: Async](fileToZip: File, outFile: File): F[File] =
    Resource
      .fromAutoCloseable(Sync[F].delay(new FileOutputStream(outFile.getAbsolutePath)))
      .flatMap(fos => Resource.fromAutoCloseable(Sync[F].delay(new ZipOutputStream(fos))))
      .use(zipFile(fileToZip, fileToZip.getName, _))
      .as(outFile)

  private def zipFile[F[_]: Async](fileToZip: File,
                                   fileName: String,
                                   zipOut: ZipOutputStream
  ): F[Unit] =
    fileToZip match {
      case f if f.isHidden => Async[F].unit
      case f if f.isDirectory =>
        Sync[F].blocking(if (fileName.endsWith("/")) {
          zipOut.putNextEntry(new ZipEntry(fileName))
          zipOut.closeEntry()
        } else {
          zipOut.putNextEntry(new ZipEntry(fileName + "/"))
          zipOut.closeEntry()
        }) >> fileToZip
          .listFiles()
          .toList
          .traverse(child => zipFile(child, fileName + "/" + child.getName, zipOut))
          .void
      case _ =>
        Sync[F].delay(zipOut.putNextEntry(new ZipEntry(fileName))) >>
          Files[F]
            .readAll(FPath(fileToZip.getAbsolutePath))
            .chunks
            .evalMap(chunk => Sync[F].blocking(zipOut.write(chunk.toArray)))
            .compile
            .drain
    }

  def convertFromZip[F[_]: Async](zippedFile: File, outFile: File): F[File] =
    Resource
      .make(Async[F].delay(new ZipInputStream(new FileInputStream(zippedFile))))(zis =>
        Async[F].delay(zis.closeEntry()) >> Async[F].delay(zis.close())
      )
      .use(zis =>
        fs2.Stream
          .repeatEval(Async[F].delay(zis.getNextEntry))
          .map(Option.apply)
          .unNoneTerminate
          .map(entry => entry -> newFile(outFile, entry))
          .evalMap {
            case (dirEntry, Some(curFile)) if dirEntry.isDirectory =>
              Async[F].blocking {
                if (!curFile.isDirectory && !curFile.mkdirs())
                  throw new IOException(s"Failed to unzip dir entry: $curFile")
              }
            case (_, Some(curFile)) =>
              Async[F]
                .delay(curFile.getParentFile)
                .flatMap(parent =>
                  Async[F].blocking {
                    if (!parent.isDirectory && !curFile.getParentFile.mkdirs())
                      throw new IOException(s"Failed to unzip file entry: $curFile")
                  }
                ) >>
                fs2.io
                  .readInputStream(Async[F].pure(zis: InputStream), 1 << 10, closeAfterUse = false)
                  .through(Files[F].writeAll(FPath(curFile.getAbsolutePath)))
                  .compile
                  .drain
          }
          .compile
          .drain
      )
      .as(outFile)

  private def newFile(destinationDir: File, zipEntry: ZipEntry): Option[File] =
    Option(new File(destinationDir, zipEntry.getName))
      .filter(_.getCanonicalPath.startsWith(destinationDir.getCanonicalPath + File.separator))
}
