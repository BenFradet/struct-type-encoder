lazy val buildSettings = Seq(
  organization := "com.github.benfradet",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.1"
)

lazy val shapelessVersion = "2.3.2"

lazy val structTypeEncoder = (project in file("."))
  .settings(name := "struct-type-encoder")
  .settings(buildSettings)
  .settings(libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion
  ))