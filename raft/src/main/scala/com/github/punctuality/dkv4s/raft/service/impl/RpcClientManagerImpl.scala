package com.github.punctuality.dkv4s.raft.service.impl

import cats.effect.{Ref, Sync}
import cats.syntax.applicativeError._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.{Monad, MonadThrow}
import com.github.punctuality.dkv4s.raft.model.{Command, LogEntry, Node, Snapshot}
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.rpc.{RpcClient, RpcClientBuilder}
import com.github.punctuality.dkv4s.raft.service.{ErrorLogging, RpcClientManager}
import com.github.punctuality.dkv4s.raft.util.Logger

class RpcClientManagerImpl[F[_]: MonadThrow: RpcClientBuilder: Logger](
  val clientsRef: Ref[F, Map[Node, RpcClient[F]]],
  val members: Seq[Node]
) extends ErrorLogging[F] with RpcClientManager[F] {

  def send(serverId: Node, voteRequest: VoteRequest): F[VoteResponse] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(voteRequest).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def send(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse] =
    errorLogging("Sending AppendEntries") {
      for {
        client  <- getClient(serverId)
        _       <- Logger[F].trace(s"Sending request $appendEntries to $serverId client")
        attempt <- client.send(appendEntries).attempt
        _       <- Logger[F].trace(s"Attempt $attempt")
        result  <- logErrors(serverId, attempt)
      } yield result
    }

  def send(serverId: Node, snapshot: InstallSnapshot): F[AppendEntriesResponse] =
    for {
      client   <- getClient(serverId)
      attempt  <- client.send(snapshot).attempt
      response <- logErrors(serverId, attempt)
    } yield response

  def send[T](serverId: Node, command: Command[T]): F[T] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(command).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def join(serverId: Node, newNode: Node): F[Boolean] =
    for {
      client  <- getClient(serverId)
      attempt <- client.join(newNode).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  private def logErrors[T](peerId: Node, result: Either[Throwable, T]): F[T] =
    result match {
      case Left(error) =>
        Logger[F].warn(s"An error during communication with $peerId. Error: $error") >>
          error.raiseError[F, T]
      case Right(value) =>
        Monad[F].pure(value)
    }

  private def getClient(serverId: Node): F[RpcClient[F]] =
    clientsRef.get.flatMap(_.get(serverId) match {
      case Some(client) => client.pure[F]
      case None =>
        RpcClientBuilder[F]
          .build(serverId)
          .flatMap(client => clientsRef.update(_ + (serverId -> client)).as(client))
    })

  def closeConnections(): F[Unit] =
    Logger[F].trace("Close all connections to other members") >>
      clientsRef.get.flatMap(_.values.toList.traverse(_.close())).void
}

object RpcClientManagerImpl {

  def build[F[_]: Monad: Sync: RpcClientBuilder: Logger](
    members: Seq[Node]
  ): F[RpcClientManagerImpl[F]] =
    Ref.of[F, Map[Node, RpcClient[F]]](Map.empty).map(new RpcClientManagerImpl[F](_, members))
}
