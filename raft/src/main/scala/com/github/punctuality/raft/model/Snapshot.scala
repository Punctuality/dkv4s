package com.github.punctuality.raft.model

import com.github.punctuality.raft.protocol.ClusterConfiguration

import java.nio.ByteBuffer

case class Snapshot(lastIndex: Long, bytes: ByteBuffer, config: ClusterConfiguration)
