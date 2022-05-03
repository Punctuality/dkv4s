ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

lazy val common = Seq(
  compilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)

lazy val engine = (project in file("engine"))
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.3.11",
      "org.rocksdb" % "rocksdbjni" % "7.1.2"
    ) ++ common
  )

lazy val cluster = (project in file("cluster"))
  .settings(
    libraryDependencies ++= common
  )
  .dependsOn(engine)

lazy val root = (project in file("."))
  .settings(name := "dkv4s")
  .aggregate(engine, cluster)
  .dependsOn(engine, cluster)
