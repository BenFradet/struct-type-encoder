lazy val buildSettings = Seq(
  organization := "com.github.benfradet",
  version := "0.7.0-SNAPSHOT",
  scalaVersion := "2.13.13"
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

lazy val shapelessVersion = "2.3.8"
lazy val sparkVersion = "3.2.1"
lazy val scalatestVersion = "3.2.11"

lazy val baseSettings = Seq(
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  ) ++ Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion
  ).map(_ % "test"),
  parallelExecution in Test := false,
  scalacOptions ++= compilerOptions
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/BenFradet/struct-type-encoder")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/BenFradet/struct-type-encoder"),
      "scm:git:git@github.com:BenFradet/struct-type-encoder.git"
    )
  ),
  pomExtra :=
    <developers>
      <developer>
        <id>BenFradet</id>
        <name>Ben Fradet</name>
        <url>https://benfradet.github.io/</url>
      </developer>
    </developers>
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val allSettings = buildSettings ++ baseSettings ++ publishSettings

lazy val structTypeEncoder = (project in file("."))
  .settings(allSettings)
  .settings(noPublishSettings)
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
  .settings(moduleName := "struct-type-encoder")
  .settings(allSettings)

lazy val benchmarks = project
  .settings(moduleName := "struct-type-encoder-benchmarks")
  .enablePlugins(JmhPlugin)
  .settings(allSettings)
  .settings(noPublishSettings)
  .settings(libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % sparkVersion))
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
