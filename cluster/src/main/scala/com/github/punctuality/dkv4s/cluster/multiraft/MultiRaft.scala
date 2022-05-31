package com.github.punctuality.dkv4s.cluster.multiraft

import cats.effect.{Async, Fiber, Ref, Resource}
import cats.effect.std.{Console, Semaphore}
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.effect.syntax.spawn._
import com.github.punctuality.dkv4s.cluster.rocks._
import com.github.punctuality.dkv4s.engine.RocksEngine
import com.github.punctuality.dkv4s.engine.utils.NativeResource.nativeResource
import com.github.punctuality.dkv4s.raft.impl.{RaftCluster, RaftImpl, SingleRaftCluster}
import com.github.punctuality.dkv4s.raft.{Cluster, Raft}
import com.github.punctuality.dkv4s.raft.model.{Command, Configuration, LogCompactionPolicy, Node}
import com.github.punctuality.dkv4s.raft.node._
import com.github.punctuality.dkv4s.raft.protocol.ClusterConfiguration
import com.github.punctuality.dkv4s.raft.rpc.{RpcServer, RpcServerBuilder}
import com.github.punctuality.dkv4s.raft.rpc.grpc.serializer.ProtoSerializer
import com.github.punctuality.dkv4s.raft.rpc.grpc.{GrpcClientBuilder, GrpcServerBuilder}
import com.github.punctuality.dkv4s.raft.service.RpcClientManager
import com.github.punctuality.dkv4s.raft.service.impl.{LeaderAnnouncerImpl, LogImpl, LogPropagatorImpl}
import com.github.punctuality.dkv4s.raft.storage.impl.memory.{MemoryLogStorage, MemorySnapshotStorage, MemoryStateStorage, MemoryStorage}
import com.github.punctuality.dkv4s.raft.storage.{LogStorage, SnapshotStorage, StateMachine, StateStorage, Storage}
import com.github.punctuality.dkv4s.raft.util.Logger
import com.github.punctuality.dkv4s.raft.util.console.ConsoleLogger
import org.rocksdb.Options
import scodec.bits.ByteVector

class MultiRaft[F[+_]: Async: Logger](val raft: Raft[F, ClusterStateMachine],
                                      val rpc: RpcServer[F],
                                      curNodeLimit: Semaphore[F],
                                      subConfigTemplate: Configuration,
                                      subLogStorage: => F[LogStorage[F]],
                                      subSnapshotStorage: => F[SnapshotStorage[F]],
                                      subStateStorage: => F[StateStorage[F]],
                                      subStateMachine: => F[StateMachine[F]]
) extends Cluster[F, ClusterStateMachine] {
  private val mainCluster  = new SingleRaftCluster[F, ClusterStateMachine](rpc, raft)
  private val procFiberRef = Ref.unsafe(Option.empty[Fiber[F, Throwable, Unit]])

  val innerClusters: Ref[F, Map[Int, SingleRaftCluster[F, StateMachine]]] =
    Ref.unsafe(Map.empty)

  private def routeByHash(bytes: ByteVector): F[SingleRaftCluster[F, StateMachine]] =
    innerClusters.get.map(remsToClusters =>
      remsToClusters.apply(bytes.hashCode % remsToClusters.size)
    )

  override def start: F[Node] =
    for {
      leader     <- mainCluster.start
      allMembers <- mainCluster.raft.membershipManager.members.map(_.toList)
      limit      <- curNodeLimit.available.map(_.toInt)
      _ <- mainCluster.raft.getCurrentState match {
             case _: LeaderNode =>
               mainCluster.execute(MaxLeaderCommand(allMembers.map(_ -> limit))) >>
                 allMembers.zipWithIndex.traverse { case (_, remainder) =>
                   mainCluster.raft.log.stateMachine.nextId.flatMap(nextRaftId =>
                     mainCluster.execute(InitClusterCommand(nextRaftId, remainder))
                   )
                 }
           }
      procFiber <-
        fs2.Stream
          .fromQueueUnterminated(mainCluster.raft.log.stateMachine.clusterQueue)
          .evalMap(requestedId => makeSubCluster(requestedId))
          .evalTap(_.start)
          .evalTap(cluster => mainCluster.execute(ConfirmRaftCommand(cluster.raft.raftId)))
          .evalTap(cluster =>
            mainCluster.raft.log.stateMachine.clusterState.get
              .map(_.apply(cluster.raft.raftId))
              .flatMap(remainder => innerClusters.update(_.updated(remainder.remainder, cluster)))
          )
          .compile
          .drain
          .start
      _ <- procFiberRef.set(Some(procFiber))
    } yield leader

  private def makeSubCluster(raftId: Int): F[SingleRaftCluster[F, StateMachine]] =
    for {
      logStorage      <- subLogStorage
      snapshotStorage <- subSnapshotStorage
      stateStorage    <- subStateStorage
      stateMachine    <- subStateMachine
      storage          = Storage(logStorage, stateStorage, snapshotStorage)
      members         <- mainCluster.raft.membershipManager.members
      curNode          = mainCluster.raft.nodeId
      config =
        subConfigTemplate.copy(local = curNode, members = members.filter(_ != curNode).toList)
      membershipManagement = mainCluster.raft.membershipManager
      clientProvider       = mainCluster.raft.clientProvider
      persistedState      <- stateStorage.retrieveState()
      appliedIndex         = persistedState.map(_.appliedIndex).getOrElse(0L)
      log <- LogImpl.build(
               raftId,
               logStorage,
               snapshotStorage,
               stateMachine,
               LogCompactionPolicy.fixedSize(100),
               membershipManagement,
               appliedIndex
             )
      announcer  <- LeaderAnnouncerImpl.build[F]
      replicator <- LogPropagatorImpl.build[F](config.local, clientProvider, log)
      heartbeat  <- Ref.of[F, Long](0L)
      nodeState <- Ref.of[F, NodeState](
                     persistedState
                       .map(_.toNodeState(raftId, config.local))
                       .getOrElse(FollowerNode(raftId, config.local, 0L))
                   )
      running <- Ref.of[F, Boolean](false)
    } yield new SingleRaftCluster[F, StateMachine](
      rpc,
      new RaftImpl[F, StateMachine](
        raftId,
        Some(curNodeLimit),
        config,
        membershipManagement,
        clientProvider,
        announcer,
        replicator,
        log,
        storage,
        nodeState,
        heartbeat,
        running
      )
    )

  override def stop: F[Unit] = mainCluster.stop

  override def join(node: Node): F[Node] =
    mainCluster
      .join(node)
      .flatTap(_ => innerClusters.get.flatMap(_.toList.traverse(_._2.join(node))))

  override def leave: F[Unit] = mainCluster.leave

  override def leader: F[Node] = mainCluster.leader

  private def routeMany[T](many: List[T],
                           key: T => ByteVector
  ): F[List[(SingleRaftCluster[F, StateMachine], List[T])]] =
    many
      .traverse(value => routeByHash(key(value)).map(_ -> value))
      .map(_.groupBy(_._1.raft.raftId))
      .map(grouped =>
        grouped.view.map { case (_, list) =>
          list.head._1 -> list.map(_._2)
        }
      )
      .map(_.toList)

  override def execute[T](command: Command[T]): F[T] =
    command match {
      case rc: RocksCommand =>
        rc match {
          case c @ WriteSingleCommand(key, _) => routeByHash(key).flatMap(_.execute(c))
          case c @ DeleteSingleCommand(key)   => routeByHash(key).flatMap(_.execute(c))
          case c @ ReadSingleCommand(key)     => routeByHash(key).flatMap(_.execute(c))
          case WriteManyCommand(many) =>
            routeMany[(ByteVector, ByteVector)](many, _._1)
              .flatMap(_.traverse { case (cluster, data) =>
                cluster.execute[Unit](WriteManyCommand(data))
              })
              .void
              .asInstanceOf[F[T]]
          case DeleteManyCommand(many) =>
            routeMany[ByteVector](many, identity)
              .flatMap(_.traverse { case (cluster, data) =>
                cluster.execute[Unit](DeleteManyCommand(data))
              })
              .void
              .asInstanceOf[F[T]]
          case ReadManyCommand(many) =>
            routeMany[ByteVector](many, identity)
              .flatMap(_.flatTraverse { case (cluster, data) =>
                cluster.execute[List[Option[ByteVector]]](ReadManyCommand(data))
              })
              .asInstanceOf[F[T]]
        }
      case otherCommand => mainCluster.execute(otherCommand)
    }
}

object MultiRaft {
  def apply[F[+_]: Async: Console](name: String,
                                   config: Configuration,
                                   subConfig: Configuration,
                                   rocksPath: String
  )(implicit
    commandSer: ProtoSerializer[Command[_]],
    configSer: ProtoSerializer[ClusterConfiguration],
    objectSer: ProtoSerializer[Any]
  ): Resource[F, MultiRaft[F]] = {
    implicit val logger: Logger[F] = ConsoleLogger[F](name, ConsoleLogger.Level.Info)

    implicit val clientBuilder: GrpcClientBuilder[F] = GrpcClientBuilder[F]
    implicit val serverBuilder: GrpcServerBuilder[F] = GrpcServerBuilder[F]

    for {
      rocksOptions <- nativeResource(Async[F].delay(new Options().setCreateIfMissing(true)))
      rocksEngine <-
        Resource eval RocksEngine[F](RocksEngine.initDB(rocksPath, rocksOptions, ttl = false))
      multiRaft = for {
                    stateMachine <- ClusterStateMachine.empty[F]
                    raftId       <- stateMachine.nextId
                    storage      <- MemoryStorage.empty[F]
                    compaction = if (config.logCompactionThreshold >= 0)
                                   LogCompactionPolicy.fixedSize(config.logCompactionThreshold)
                                 else LogCompactionPolicy.noCompaction
                    mainRaft <-
                      RaftImpl.build[F, ClusterStateMachine](
                        raftId,
                        config,
                        storage,
                        stateMachine,
                        compaction
                      )
                    initMap: Map[Int, Raft[F, StateMachine]] = Map(mainRaft.raftId -> mainRaft)
                    raftMap                                 <- Ref.of(initMap)
                    server                                  <- RpcServerBuilder[F].build(config.local, raftMap)
                    limitSemaphore                          <- Semaphore[F](1)
                  } yield new MultiRaft[F](
                    mainRaft,
                    server,
                    limitSemaphore,
                    subConfig,
                    MemoryLogStorage.empty[F],
                    MemorySnapshotStorage.empty[F],
                    MemoryStateStorage.empty[F],
                    Async[F].delay(RocksKVStateMachine[F](rocksEngine))
                  )
      cluster <- Resource.make(multiRaft)(_.stop)
    } yield cluster
  }
}
