package com.github.punctuality.dkv4s.raft.model

import com.github.punctuality.dkv4s.raft.protocol.ClusterConfiguration
import java.nio.ByteBuffer

case class Snapshot(lastIndex: Long, bytes: ByteBuffer, config: ClusterConfiguration)
