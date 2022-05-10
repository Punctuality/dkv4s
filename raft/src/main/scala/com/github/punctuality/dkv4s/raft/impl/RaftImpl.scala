package com.github.punctuality.dkv4s.raft.impl

import cats.effect.{Async, Clock, Concurrent, Ref, Temporal}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monad._
import cats.{Monad, MonadError}
import com.github.punctuality.dkv4s.raft.Raft
import com.github.punctuality.dkv4s.raft.model.{Configuration, LogCompactionPolicy, Node}
import com.github.punctuality.dkv4s.raft.node.{FollowerNode, LeaderNode, NodeState}
import com.github.punctuality.dkv4s.raft.rpc.RpcClientBuilder
import com.github.punctuality.dkv4s.raft.service.impl._
import com.github.punctuality.dkv4s.raft.storage.{StateMachine, Storage}
import com.github.punctuality.dkv4s.raft.util.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class RaftImpl[F[_]: Async](val config: Configuration,
                            val membershipManager: ClusterConfigStorageImpl[F],
                            val clientProvider: RpcClientManagerImpl[F],
                            val leaderAnnouncer: LeaderAnnouncerImpl[F],
                            val logReplicator: LogPropagatorImpl[F],
                            val log: LogImpl[F],
                            val storage: Storage[F],
                            stateRef: Ref[F, NodeState],
                            lastHeartbeatRef: Ref[F, Long],
                            isRunning: Ref[F, Boolean]
)(implicit val ME: MonadError[F, Throwable], val logger: Logger[F])
  extends Raft[F] {

  override val nodeId: Node = config.local

  override def setRunning(running: Boolean): F[Unit] =
    isRunning.set(running)

  override def getRunning: F[Boolean] =
    isRunning.get

  override def getCurrentState: F[NodeState] =
    stateRef.get

  override def setCurrentState(state: NodeState): F[Unit] =
    stateRef.set(state)

  override def background[A](fa: => F[A]): F[Unit] =
    Concurrent[F].start(fa).void

  override def updateLastHeartbeat(): F[Unit] =
    logger.trace(s"Update Last heartbeat time") >>
      Clock[F].monotonic.flatMap(t => lastHeartbeatRef.set(t.toMillis))

  override def electionTimeoutElapsed: F[Boolean] =
    for {
      node <- getCurrentState
      lh   <- lastHeartbeatRef.get
      now  <- Clock[F].monotonic
    } yield node.isInstanceOf[LeaderNode] || (now.toMillis - lh < config.heartbeatTimeoutMillis)

  override def delayElection(): F[Unit] =
    for {
      millis <- random(config.electionMinDelayMillis, config.electionMaxDelayMillis)
      delay   = FiniteDuration(millis, TimeUnit.MILLISECONDS)
      _      <- logger.trace(s"Delay to start the election $delay")
      _      <- Temporal[F].sleep(delay)
    } yield ()

  override def schedule(delay: FiniteDuration)(fa: => F[Unit]): F[Unit] =
    Monad[F]
      .foreverM(Temporal[F].sleep(delay) >> fa)
      .whileM_(isRunning.get)

  private def random(min: Int, max: Int): F[Int] =
    Async[F].delay(min + scala.util.Random.nextInt(max - min))
}

object RaftImpl {

  def build[F[_]: Async: RpcClientBuilder: Logger](config: Configuration,
                                                   storage: Storage[F],
                                                   stateMachine: StateMachine[F],
                                                   compactionPolicy: LogCompactionPolicy[F]
  ): F[RaftImpl[F]] =
    for {
      persistedState <- storage.stateStorage.retrieveState()
      nodeState =
        persistedState.map(_.toNodeState(config.local)).getOrElse(FollowerNode(config.local, 0L))
      appliedIndex    = persistedState.map(_.appliedIndex).getOrElse(0L)
      clientProvider <- RpcClientManagerImpl.build[F](config.members)
      membership     <- ClusterConfigStorageImpl.build[F](config.members.toSet + config.local)
      log <- LogImpl
               .build[F](
                 storage.logStorage,
                 storage.snapshotStorage,
                 stateMachine,
                 compactionPolicy,
                 membership,
                 appliedIndex
               )
      replicator <- LogPropagatorImpl.build[F](config.local, clientProvider, log)
      announcer  <- LeaderAnnouncerImpl.build[F]
      heartbeat  <- Ref.of[F, Long](0L)
      ref        <- Ref.of[F, NodeState](nodeState)
      running    <- Ref.of[F, Boolean](false)
    } yield new RaftImpl[F](
      config,
      membership,
      clientProvider,
      announcer,
      replicator,
      log,
      storage,
      ref,
      heartbeat,
      running
    )

}