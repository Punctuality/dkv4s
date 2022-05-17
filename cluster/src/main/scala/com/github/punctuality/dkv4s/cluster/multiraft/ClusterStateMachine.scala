package com.github.punctuality.dkv4s.cluster.multiraft

import cats.effect.std.Queue
import cats.effect.{Async, Ref, Sync}
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.github.punctuality.dkv4s.raft.model.{Node, ReadCommand, WriteCommand}
import com.github.punctuality.dkv4s.raft.storage.StateMachine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

case class MaxLeaderCommand(membersAllowance: List[(Node, Int)]) extends WriteCommand[Unit]
case class InitClusterCommand(newRaftId: Int, remainder: Int)    extends WriteCommand[Unit]
case class ConfirmRaftCommand(raftId: Int)                       extends WriteCommand[Unit]

case class IsConfirmedCommand(raftId: Int)       extends ReadCommand[Boolean]
case class MaxLeadersForCommand(node: Node)      extends ReadCommand[Int]
case class RemainderForGroupCommand(raftId: Int) extends ReadCommand[Int]

case class RaftState(remainder: Int, confirmed: Boolean)

class ClusterStateMachine[F[_]: Sync](lastIndex: Ref[F, Long],
                                      val raftCounter: Ref[F, Int],
                                      val maxLeaders: Ref[F, Map[Node, Int]],
                                      val clusterState: Ref[F, Map[Int, RaftState]],
                                      val clusterQueue: Queue[F, Int]
) extends StateMachine[F] {
  override def applyWrite: WriteHandler = {
    case (index, MaxLeaderCommand(allowance)) =>
      maxLeaders.set(allowance.toMap) >> lastIndex.set(index)
    case (index, InitClusterCommand(newRaftId, remainder)) =>
      clusterState.update(
        _.updated(newRaftId, RaftState(remainder, confirmed = false))
      ) >> clusterQueue.offer(newRaftId) >> lastIndex.set(index)
    case (index, ConfirmRaftCommand(raftId)) =>
      clusterState.update(_.updatedWith(raftId)(_.map(_.copy(confirmed = true)))) >>
        lastIndex.set(index)
  }

  override def applyRead: ReadHandler = {
    case IsConfirmedCommand(raftId) =>
      clusterState.get.map(_.get(raftId).exists(_.confirmed))
    case MaxLeadersForCommand(node) =>
      maxLeaders.get.map(_.getOrElse(node, 0))
    case RemainderForGroupCommand(raftId) =>
      clusterState.get.map(_.get(raftId).map(_.remainder).getOrElse(-1))
  }

  override def appliedIndex: F[Long] = lastIndex.get

  override def takeSnapshot: F[(Long, ByteBuffer)] =
    for {
      state   <- clusterState.get
      leaders <- maxLeaders.get
      counter <- raftCounter.get
      index   <- lastIndex.get
      bytes    = serialize((state, leaders, counter))
    } yield (index, bytes)

  override def restoreSnapshot(index: Long, bytes: ByteBuffer): F[Unit] =
    Sync[F].delay(deserialize(bytes)).flatMap { case (state, leaders, counter) =>
      lastIndex.set(index) >>
        clusterState.set(state) >>
        maxLeaders.set(leaders) >>
        raftCounter.set(counter)
    }

  def nextId: F[Int] =
    raftCounter.getAndUpdate(_ + 1)

  type SerType = (Map[Int, RaftState], Map[Node, Int], Int)

  private def serialize(items: SerType): ByteBuffer = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(items)
    oos.close()

    ByteBuffer.wrap(stream.toByteArray)
  }

  private def deserialize(bytes: ByteBuffer): SerType = {

    val ois      = new ObjectInputStream(new ByteArrayInputStream(bytes.array()))
    val response = ois.readObject().asInstanceOf[SerType]
    ois.close()

    response
  }
}

object ClusterStateMachine {
  def empty[F[_]: Async]: F[ClusterStateMachine[F]] = for {
    index   <- Ref.of(0L)
    counter <- Ref.of(0)
    leaders <- Ref.of(Map.empty[Node, Int])
    state   <- Ref.of(Map.empty[Int, RaftState])
    queue   <- Queue.unbounded[F, Int]
  } yield new ClusterStateMachine[F](index, counter, leaders, state, queue)
}
