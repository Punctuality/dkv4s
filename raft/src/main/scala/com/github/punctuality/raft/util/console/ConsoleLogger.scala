package com.github.punctuality.raft.util.console

import cats.Monad
import cats.effect.kernel.Clock
import enumeratum.values._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.effect.std.Console
import com.github.punctuality.raft.util.Logger
import com.github.punctuality.raft.util.console.ConsoleLogger.Level

case class ConsoleLogger[F[_]: Console: Monad: Clock](loggingLevel: ConsoleLogger.Level)
  extends Logger[F] {
  import ConsoleLogger.Level._

  private def procLevel(opLevel: ConsoleLogger.Level)(op: => F[Unit]): F[Unit] =
    if (opLevel.value > loggingLevel.value) ().pure[F] else op

  private def logMsg(
    opLevel: ConsoleLogger.Level
  )(msg: => String, err: Option[Throwable], ctx: Option[Map[String, String]]): F[Unit] =
    procLevel(opLevel)(
      Clock[F].realTime.flatMap(time =>
        (opLevel match {
          case Level.Error => Console[F].errorln[String](_)
          case _           => Console[F].println[String](_)
        }).apply(
          s"[${time.toMillis}] ($opLevel) - $msg" + err.fold("")(e => s" (${e.getMessage})") + ctx
            .fold("")(_.mkString("\n Context:\n", ",\n", ""))
        ) >>
          err.fold(().pure[F])(Console[F].printStackTrace)
      )
    )

  override def trace(msg: => String): F[Unit]               = logMsg(Trace)(msg, None, None)
  override def trace(msg: => String, e: Throwable): F[Unit] = logMsg(Trace)(msg, Some(e), None)
  override def trace(msg: => String, ctx: Map[String, String]): F[Unit] =
    logMsg(Trace)(msg, None, Some(ctx))
  override def trace(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] =
    logMsg(Trace)(msg, Some(e), Some(ctx))
  override def debug(msg: => String): F[Unit]               = logMsg(Debug)(msg, None, None)
  override def debug(msg: => String, e: Throwable): F[Unit] = logMsg(Debug)(msg, Some(e), None)
  override def debug(msg: => String, ctx: Map[String, String]): F[Unit] =
    logMsg(Debug)(msg, None, Some(ctx))
  override def debug(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] =
    logMsg(Debug)(msg, Some(e), Some(ctx))
  override def info(msg: => String): F[Unit]               = logMsg(Info)(msg, None, None)
  override def info(msg: => String, e: Throwable): F[Unit] = logMsg(Info)(msg, Some(e), None)
  override def info(msg: => String, ctx: Map[String, String]): F[Unit] =
    logMsg(Info)(msg, None, Some(ctx))
  override def info(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] =
    logMsg(Info)(msg, Some(e), Some(ctx))
  override def warn(msg: => String): F[Unit]               = logMsg(Warn)(msg, None, None)
  override def warn(msg: => String, e: Throwable): F[Unit] = logMsg(Warn)(msg, Some(e), None)
  override def warn(msg: => String, ctx: Map[String, String]): F[Unit] =
    logMsg(Warn)(msg, None, Some(ctx))
  override def warn(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] =
    logMsg(Warn)(msg, Some(e), Some(ctx))
  override def error(msg: => String): F[Unit]               = logMsg(Error)(msg, None, None)
  override def error(msg: => String, e: Throwable): F[Unit] = logMsg(Error)(msg, Some(e), None)
  override def error(msg: => String, ctx: Map[String, String]): F[Unit] =
    logMsg(Error)(msg, None, Some(ctx))
  override def error(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] =
    logMsg(Error)(msg, Some(e), Some(ctx))
}

object ConsoleLogger {
  sealed abstract class Level(val value: Byte) extends ByteEnumEntry
  object Level extends ByteEnum[Level] {
    case object Trace extends Level(5)
    case object Debug extends Level(4)
    case object Info  extends Level(3)
    case object Warn  extends Level(2)
    case object Error extends Level(1)

    override def values: IndexedSeq[Level] = findValues
  }
}
