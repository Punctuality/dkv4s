package com.github.punctuality.dkv4s.raft

import cats.MonadError
import cats.effect.Concurrent
import cats.effect.std.Semaphore
import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.node._
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.service._
import com.github.punctuality.dkv4s.raft.storage.{StateMachine, Storage}
import com.github.punctuality.dkv4s.raft.util.Logger

import scala.concurrent.duration.FiniteDuration

abstract class Raft[F[_]: Concurrent, +SM[X[_]] <: StateMachine[X]] extends ErrorLogging[F] {
  implicit val logger: Logger[F]
  implicit val ME: MonadError[F, Throwable]

  val nodeId: Node

  val raftId: Int
  val leadersLimit: Option[Semaphore[F]]

  val config: Configuration
  val log: Log[F, SM]
  val storage: Storage[F]

  val leaderAnnouncer: LeaderAnnouncer[F]

  val clientProvider: RpcClientManager[F]

  val membershipManager: ClusterConfigStorage[F]

  val logReplicator: LogPropagator[F]

  def setRunning(isRunning: Boolean): F[Unit]

  def getRunning: F[Boolean]

  def getCurrentState: F[NodeState]

  def setCurrentState(state: NodeState): F[Unit]

  def background[A](fa: => F[A]): F[Unit]

  def updateLastHeartbeat(): F[Unit]

  def electionTimeoutElapsed: F[Boolean]

  def delayElection(): F[Unit]

  def schedule(delay: FiniteDuration)(fa: => F[Unit]): F[Unit]

  def initialize: F[Unit] = log.initialize

  def start: F[Node]

  def join(node: Node): F[Node]

  def stop: F[Unit]

  def leave: F[Unit]

  def listen: F[Node]

  def onVote(msg: VoteRequest): F[VoteResponse]

  def onVoteResp(msg: VoteResponse): F[Unit]

  def onAppendEntries(msg: AppendEntries): F[AppendEntriesResponse]

  def onAppendResponse(msg: AppendEntriesResponse): F[Unit]

  def onSnapshot(msg: InstallSnapshot): F[AppendEntriesResponse]

  def addMember(member: Node): F[Unit]

  def removeMember(member: Node): F[Unit]

  def onCommand[T](command: Command[T]): F[T]
}
