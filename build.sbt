ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

fork := true
outputStrategy := Some(StdoutOutput)
connectInput := true

lazy val common = Seq(
  compilerPlugin("org.typelevel" % "kind-projector"     % "0.13.2" cross CrossVersion.full),
  compilerPlugin("com.olegpy"   %% "better-monadic-for" % "0.3.1"),
  "org.typelevel" %% "cats-effect"      % "3.3.11",
  "co.fs2"        %% "fs2-core"         % "3.2.7",
  "org.scodec"     % "scodec-core_2.13" % "1.11.9"
)

lazy val engine = (project in file("engine"))
  .settings(libraryDependencies ++= Seq("org.rocksdb" % "rocksdbjni" % "7.1.2") ++ common)

lazy val raft = (project in file("raft"))
  .settings(
    libraryDependencies ++= Seq(
      "io.grpc"       % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion,
      "co.fs2"       %% "fs2-core"          % "3.2.7",
      "co.fs2"       %% "fs2-io"            % "3.2.7",
      "co.fs2"       %% "fs2-scodec"        % "3.2.7",
      "io.scalaland" %% "chimney"           % "0.6.1",
      "com.beachape" %% "enumeratum"        % "1.7.0",
      "com.beachape" %% "enumeratum-cats"   % "1.7.0"
    ) ++ common,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.Grpc,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.Fs2Grpc
  )
  .enablePlugins(Fs2Grpc)

lazy val cluster = (project in file("cluster"))
  .settings(
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases",
    libraryDependencies ++= Seq("com.storm-enroute" %% "scalameter" % "0.21") ++ common,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    Test / parallelExecution := false
  )
  .dependsOn(engine, raft)

lazy val root = (project in file("."))
  .settings(name := "dkv4s")
  .aggregate(engine, raft, cluster)
  .dependsOn(engine, raft, cluster)
