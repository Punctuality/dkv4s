package com.github.punctuality.dkv4s.raft.impl

import cats.effect.std.Semaphore
import cats.effect.{Async, Clock, Concurrent, Deferred, Ref, Temporal}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monad._
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.traverse._
import cats.{Applicative, Monad, MonadError}
import com.github.punctuality.dkv4s.raft.{Raft, model}
import com.github.punctuality.dkv4s.raft.model._
import com.github.punctuality.dkv4s.raft.node._
import com.github.punctuality.dkv4s.raft.protocol._
import com.github.punctuality.dkv4s.raft.rpc.RpcClientBuilder
import com.github.punctuality.dkv4s.raft.service._
import com.github.punctuality.dkv4s.raft.service.impl._
import com.github.punctuality.dkv4s.raft.storage.{StateMachine, Storage}
import com.github.punctuality.dkv4s.raft.util.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class RaftImpl[F[_]: Async, +SM[X[_]] <: StateMachine[X]](
  val raftId: Int,
  val leadersLimit: Option[Semaphore[F]],
  val config: Configuration,
  val membershipManager: ClusterConfigStorage[F],
  val clientProvider: RpcClientManager[F],
  val leaderAnnouncer: LeaderAnnouncer[F],
  val logReplicator: LogPropagator[F],
  val log: Log[F, SM],
  val storage: Storage[F],
  stateRef: Ref[F, NodeState],
  lastHeartbeatRef: Ref[F, Long],
  isRunning: Ref[F, Boolean]
)(implicit val ME: MonadError[F, Throwable], val logger: Logger[F])
  extends Raft[F, SM] {

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

  def start: F[Node] =
    errorLogging("Starting Cluster") {
      for {
        _ <- setRunning(true)
        _ <- logger.info("Cluster is starting")
        //_ <- delayElection()
        node   <- getCurrentState
        _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
        _      <- scheduleElection()
        _      <- scheduleHeartbeat()
        _      <- logger.trace("Waiting for the leader to be elected.")
        leader <- leaderAnnouncer.listen()
        _      <- logger.info(s"A Leader is elected. Leader: '$leader'")
      } yield leader
    }

  def join(node: Node): F[Node] =
    errorLogging("Joining to a cluster") {
      for {
        _      <- setRunning(true)
        _      <- logger.info("Cluster is joining")
        res    <- clientProvider.join(node, raftId, nodeId)
        _      <- logger.trace(s"CLuster is joined to $node $res")
        node   <- getCurrentState
        _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
        _      <- scheduleElection()
        _      <- scheduleHeartbeat()
        _      <- logger.trace("Waiting for the leader to be elected.")
        leader <- leaderAnnouncer.listen()
        _      <- logger.info(s"A Leader is elected. Leader: '$leader'")
      } yield leader
    }

  def stop: F[Unit] =
    errorLogging("Stopping a Cluster") {
      for {
        _ <- logger.info("Stopping the cluster")
        _ <- setRunning(false)
        _ <- clientProvider.closeConnections()
        _ <- logger.info("Cluster stopped")
      } yield ()
    }

  def leave: F[Unit] =
    errorLogging("Leaving a cluster") {
      for {
        _ <- logger.info(s"Node $nodeId is leaving the cluster")
        _ <- removeMember(nodeId)
        _ <- logger.info(s"Node $nodeId left the cluster.")
        _ <- setRunning(false)
      } yield ()
    }

  def listen: F[Node] =
    errorLogging("Waiting for the Leader to be elected") {
      leaderAnnouncer.listen()
    }

  def onVote(msg: VoteRequest): F[VoteResponse] =
    errorLogging("Receiving VoteRequest") {
      for {
        _ <- logger.trace(
               s"A Vote request received from ${msg.nodeId}, Term: ${msg.lastLogTerm}, $msg"
             )
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        result   <- modifyState(_.onVoteRequest(logState, config, msg))

        (response, actions) = result

        _ <- runActions(actions)
        _ <- logger.trace(s"Vote response to the request $response")
        _ <- if (response.voteGranted) updateLastHeartbeat() else Monad[F].unit
      } yield response
    }

  def onVoteResp(msg: VoteResponse): F[Unit] =
    errorLogging("Receiving VoteResponse") {
      for {
        _ <- logger.trace(
               s"A Vote response received from ${msg.nodeId}, Granted: ${msg.voteGranted}, $msg"
             )
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        actions  <- modifyState(_.onVoteResponse(logState, config, msg))
        _        <- runActions(actions)
      } yield ()
    }

  def onAppendEntries(msg: AppendEntries): F[AppendEntriesResponse] =
    errorLogging(
      s"Receiving an AppendEntries Term: ${msg.term} PreviousLogIndex:${msg.prevLogIndex}"
    ) {
      for {
        _ <-
          logger.trace(
            s"AppendEntries request received from ${msg.leaderId}, contains ${msg.entries.size} entries, $msg"
          )
        logState      <- log.state
        localPreEntry <- log.get(msg.prevLogIndex)
        config        <- membershipManager.getClusterConfiguration
        result        <- modifyState(_.onEntries(logState, config, msg, localPreEntry))
        _             <- updateLastHeartbeat()

        (response, actions) = result
        _                  <- runActions(actions)

        appended <-
          if (response.success) {
            logger.trace(s"Appending entries ${msg.entries}...") >>
              log.appendEntries(msg.entries, msg.prevLogIndex, msg.leaderCommit)
          } else
            Monad[F].pure(false)
        _ <- if (appended) storeState() else Monad[F].unit

        _ <- logger.trace(s"Did append? $appended (${response.success})")
        _ <- logger.trace(s"Append entries response $response")
        _ <- logger.trace(s"Actions $actions")
      } yield response
    }

  def onAppendResponse(msg: AppendEntriesResponse): F[Unit] =
    errorLogging("Receiving AppendEntriesResponse") {
      for {
        _        <- logger.trace(s"A AppendEntriesResponse received from ${msg.nodeId}. $msg")
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        actions  <- modifyState(_.onEntriesResp(logState, config, msg))
        _        <- logger.trace(s"Actions $actions")
        _        <- runActions(actions)
      } yield ()
    }

  def onSnapshot(msg: InstallSnapshot): F[AppendEntriesResponse] =
    errorLogging("Receiving InstallSnapshot") {
      for {
        _        <- log.installSnapshot(msg.snapshot, msg.lastEntry)
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        response <- modifyState(_.onSnapshotInstalled(logState, config))
      } yield response
    }

  def addMember(member: Node): F[Unit] =
    for {
      config <- membershipManager.getClusterConfiguration
      _      <- addMember(config, member)
    } yield ()

  private def addMember(config: ClusterConfiguration, member: Node): F[Unit] =
    if (config.members.contains(member)) {
      Applicative[F].unit
    } else {
      val oldMembers = config.members
      val newMembers = oldMembers + member
      val newConfig  = JointClusterConfiguration(oldMembers, newMembers)

      for {
        _ <- membershipManager.setClusterConfiguration(newConfig)
        _ <- logger.trace(s"Committing a joint configuration $newConfig")
        _ <- onCommand[Unit](JointConfigurationCommand(oldMembers, newMembers))
        _ <- logger.trace("Joint configuration is committed")
        _ <- onCommand[Unit](NewConfigurationCommand(newMembers))
        _ <- logger.trace("New configuration is committed")
      } yield ()
    }

  def removeMember(member: Node): F[Unit] =
    for {
      config <- membershipManager.getClusterConfiguration
      _      <- removeMember(config, member)
    } yield ()

  private def removeMember(config: ClusterConfiguration, member: Node): F[Unit] =
    if (!config.members.contains(member)) {
      Applicative[F].unit
    } else {

      val oldMembers = config.members.toSet
      val newMembers = oldMembers - member
      val newConfig  = JointClusterConfiguration(oldMembers, newMembers)

      for {
        _ <- membershipManager.setClusterConfiguration(newConfig)
        _ <- logger.trace(s"Committing a joint configuration $newConfig")
        _ <- onCommand[Unit](model.JointConfigurationCommand(oldMembers, newMembers))
        _ <- logger.trace("Joint configuration is committed")
        _ <- onCommand[Unit](model.NewConfigurationCommand(newMembers))
        _ <- logger.trace("New configuration is committed")
      } yield ()
    }

  def onCommand[T](command: Command[T]): F[T] =
    errorLogging("Receiving Command") {
      command match {
        case command: ReadCommand[T] =>
          for {
            _      <- logger.trace(s"A read comment received $command")
            state_ <- getCurrentState
            result <- onReadCommand(state_, command)
          } yield result

        case command: WriteCommand[T] =>
          for {
            deferred  <- Deferred[F, T]
            state_    <- getCurrentState
            _         <- logger.trace(s"A write command received $command (curState: $state_)")
            config    <- membershipManager.getClusterConfiguration
            actions   <- onWriteCommand(state_, config, command, deferred)
            _         <- runActions(actions)
            tryResult <- deferred.tryGet
            result <- tryResult match {
                        case Some(value) => value.pure[F]
                        case None        => Concurrent[F].cede >> deferred.get
                      }
          } yield result
      }
    }

  private def onReadCommand[T](node: NodeState, command: ReadCommand[T]): F[T] =
    node match {
      case _: LeaderNode =>
        for {
          _   <- logger.trace("Current node is the leader, it is running the read command")
          res <- log.applyReadCommand(command)
        } yield res

      case _: FollowerNode if config.followerAcceptRead =>
        for {
          _   <- logger.trace("Current node is a follower, it is running the read command")
          res <- log.applyReadCommand(command)
        } yield res

      case _ =>
        for {
          _        <- logger.trace("Read command has to be ran on the leader node")
          leader   <- leaderAnnouncer.listen()
          _        <- logger.trace(s"The current leader is $leader")
          response <- clientProvider.sendCommand(leader, raftId, command)
          _        <- logger.trace("Response for the read command received from the leader")
        } yield response
    }

  private def onWriteCommand[T](node: NodeState,
                                cluster: ClusterConfiguration,
                                command: WriteCommand[T],
                                deferred: Deferred[F, T]
  ): F[List[Action]] =
    node match {
      case LeaderNode(`raftId`, _, term, _, _) if cluster.members.size == 1 =>
        for {
          _         <- logger.trace(s"Appending the command to the log - ${cluster.members}")
          entry     <- log.append(term, command, deferred)
          _         <- logger.trace(s"Entry appended $entry")
          committed <- log.commitLogs(Map(nodeId -> entry.index))
          _         <- if (committed) storeState() else Monad[F].unit
        } yield List.empty
      case LeaderNode(`raftId`, _, term, _, _) =>
        for {
          _ <- logger.trace(s"Appending the command to the log ${cluster.members}")
          _ <- log.append(term, command, deferred)
          _ <- log.getCommitIndex.flatMap(i => logger.trace(s"After appending CI is: $i"))
        } yield node.onReplicateLog(cluster)
      case _ =>
        for {
          _        <- logger.trace("Write commands should be forwarded to the leader node.")
          leader   <- leaderAnnouncer.listen()
          _        <- logger.trace(s"The current leader is $leader.")
          response <- clientProvider.sendCommand(leader, raftId, command)
          _        <- logger.trace("Response for the write command received from the leader")
          _        <- deferred.complete(response)
        } yield List.empty
    }

  private def runActions(actions: List[Action]): F[Unit] =
    actions.traverse(action => runAction(action).attempt) >> Monad[F].unit

  private def runAction(action: Action): F[Unit] =
    action match {
      case RequestForVote(peerId, request) =>
        background {
          for {
            _        <- logger.trace(s"Sending a vote request to $peerId. Request: $request")
            response <- clientProvider.sendVote(peerId, request)
            _        <- onVoteResp(response)
          } yield response
        }

      case ReplicateLog(peerId, term, nextIndex) =>
        background {
          errorLogging(s"Replicating logs to $peerId, Term: $term, NextIndex: $nextIndex") {
            for {
              response <- logReplicator.propagateLogs(raftId, peerId, term, nextIndex)
              _        <- onAppendResponse(response)
            } yield ()
          }
        }

      case StoreState =>
        storeState()

      case CommitLogs(matchIndex) =>
        for {
          committed <- log.commitLogs(matchIndex)
          _         <- if (committed) storeState() else Monad[F].unit
        } yield ()

      case AnnounceLeader(leaderId, true) =>
        leaderAnnouncer.reset() >> leaderAnnouncer.announce(leaderId)

      case AnnounceLeader(leaderId, false) =>
        logger.trace("Announcing a new leader without resetting ") >>
          leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
        leaderAnnouncer.reset()

    }

  private def storeState(): F[Unit] =
    for {
      _        <- logger.trace("Storing the new state in the storage")
      logState <- log.state
      node     <- getCurrentState
      _ <- storage.stateStorage.persistState(
             node.toPersistedState.copy(appliedIndex = logState.lastAppliedIndex)
           )
    } yield ()

  private def runElection(): F[Unit] =
    for {
      _        <- delayElection()
      logState <- log.state
      config   <- membershipManager.getClusterConfiguration
      allowed  <- leadersLimit.fold(true.pure)(_.tryAcquire)
      actions  <- modifyState(_.onElectionTimer(logState, config, allowed))
      _        <- runActions(actions)
    } yield ()

  private def scheduleHeartbeat(): F[Unit] =
    background {
      schedule(FiniteDuration(config.heartbeatIntervalMillis, TimeUnit.MILLISECONDS)) {
        for {
          _      <- Logger[F].trace("Sending heartbeat")
          node   <- getCurrentState
          config <- membershipManager.getClusterConfiguration
          actions = if (node.isInstanceOf[LeaderNode]) node.onReplicateLog(config) else List.empty
          _      <- runActions(actions)
        } yield ()
      }
    }

  private def scheduleElection(): F[Unit] =
    background {
      schedule(FiniteDuration(config.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS)) {
        for {
          alive <- electionTimeoutElapsed
          _     <- if (alive) Monad[F].unit else runElection()
        } yield ()
      }
    }

  private def modifyState[B](f: NodeState => (NodeState, B)): F[B] =
    for {
      prevState         <- getCurrentState
      (newState, result) = f(prevState)
      _                 <- setCurrentState(newState)
      _ <- (prevState, newState) match {
             case (_: LeaderNode, _: FollowerNode)  => leadersLimit.fold(().pure)(_.release)
             case (_: CandidateNode, _: LeaderNode) => leadersLimit.fold(().pure)(_.acquire)
             case _                                 => ().pure
           }
    } yield result
}

object RaftImpl {

  def build[F[_]: Async: RpcClientBuilder: Logger, SM[X[_]] <: StateMachine[X]](
    raftId: Int,
    config: Configuration,
    storage: Storage[F],
    stateMachine: SM[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): F[Raft[F, SM]] =
    for {
      persistedState <- storage.stateStorage.retrieveState()
      nodeState =
        persistedState
          .map(_.toNodeState(raftId, config.local))
          .getOrElse(FollowerNode(raftId, config.local, 0L))
      appliedIndex    = persistedState.map(_.appliedIndex).getOrElse(0L)
      clientProvider <- RpcClientManagerImpl.build[F](config.members)
      membership     <- ClusterConfigStorageImpl.build[F](config.members.toSet + config.local)
      log <- LogImpl
               .build[F, SM](
                 raftId,
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
    } yield new RaftImpl[F, SM](
      raftId,
      None,
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
