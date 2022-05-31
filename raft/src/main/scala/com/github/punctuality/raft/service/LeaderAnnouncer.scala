package com.github.punctuality.raft.service

import com.github.punctuality.raft.model.Node

trait LeaderAnnouncer[F[_]] {

  def announce(leader: Node): F[Unit]

  def reset(): F[Unit]

  def listen(): F[Node]
}
