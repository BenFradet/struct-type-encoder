lazy val buildSettings = Seq(
  organization := "com.github.benfradet",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val compilerOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:higherKinds",
  "-unchecked",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Xlint"
)

lazy val shapelessVersion = "2.3.2"
lazy val sparkVersion = "2.1.0"
lazy val scalatestVersion = "3.0.1"

lazy val baseSettings = Seq(
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
  ) ++ Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion
  ).map(_ % "test"),
  scalacOptions ++= compilerOptions
)

lazy val allSettings = buildSettings ++ baseSettings

lazy val structTypeEncoder = (project in file("."))
  .settings(moduleName := "struct-type-encoder")
  .settings(allSettings)
  .settings(
    initialCommands in console :=
      """
        |import ste.StructTypeEncoder
        |import ste.StructTypeEncoder._
      """.stripMargin
  )
  .aggregate(core, benchmarks)
  .dependsOn(core)

lazy val core = project
  .settings(moduleName := "struct-type-encoder-core")
  .settings(allSettings)

lazy val benchmarks = project
  .settings(moduleName := "struct-type-encoder-benchmarks")
  .enablePlugins(JmhPlugin)
  .settings(allSettings)
  .settings(
    javaOptions in run ++= Seq(
      "-Djava.net.preferIPv4Stack=true",
      "-XX:+AggressiveOpts",
      "-XX:+UseParNewGC",
      "-XX:+UseConcMarkSweepGC",
      "-XX:+CMSParallelRemarkEnabled",
      "-XX:+CMSClassUnloadingEnabled",
      "-XX:ReservedCodeCacheSize=128m",
      "-Xss8M",
      "-Xms512M",
      "-XX:SurvivorRatio=128",
      "-XX:MaxTenuringThreshold=0",
      "-Xss8M",
      "-Xms512M",
      "-Xmx2G",
      "-server"
    )
  )
  .dependsOn(core)