package com.github.punctuality.dkv4s.raft.model

import com.github.punctuality.dkv4s.raft.protocol.{ClusterConfiguration, JointClusterConfiguration, NewClusterConfiguration}

//TODO Deal with this Aux type
sealed trait Command[OUT] extends Serializable
trait ReadCommand[OUT]    extends Command[OUT]
trait WriteCommand[OUT]   extends Command[OUT]

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
