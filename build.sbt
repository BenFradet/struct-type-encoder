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

lazy val structTypeEncoder = (project in file("."))
  .settings(name := "struct-type-encoder")
  .settings(buildSettings)
  .settings(
    initialCommands in console :=
      """
        |import benfradet.ste._
      """.stripMargin
  )
  .settings(libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
  ) ++ Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion
  ).map(_ % "test"))
  .settings(scalacOptions ++= compilerOptions)