package com.github.punctuality.dkv4s.raft.service

import com.github.punctuality.dkv4s.raft.model.Node

trait LeaderAnnouncer[F[_]] {

  def announce(leader: Node): F[Unit]

  def reset(): F[Unit]

  def listen(): F[Node]
}
