package com.github.punctuality.dkv4s.raft.rpc

import cats.effect.{Resource, Sync}
import cats.syntax.functor._
import io.grpc.Server

trait RpcServer[F[_]] {
  val server: Server

  def start: F[Unit]
  def stop: F[Unit]
}
