package com.github.punctuality.dkv4s.raft.model

import com.github.punctuality.dkv4s.raft.protocol.{ClusterConfiguration, JointClusterConfiguration, NewClusterConfiguration}
import scodec.codecs.{Discriminated, uint8}

//TODO Deal with this Aux type
sealed trait Command[OUT] extends Serializable
trait ReadCommand[OUT]    extends Command[OUT]
trait WriteCommand[OUT]   extends Command[OUT]

object Command {
  implicit def discriminated[OUT]: Discriminated[Command[OUT], Int] = Discriminated(uint8)
}

sealed trait ClusterConfigurationCommand extends WriteCommand[Unit] {
  def toConfig: ClusterConfiguration
}

case class JointConfigurationCommand(oldMembers: Set[Node], newMembers: Set[Node])
  extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = JointClusterConfiguration(oldMembers, newMembers)
}

case class NewConfigurationCommand(members: Set[Node]) extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = NewClusterConfiguration(members)
}
