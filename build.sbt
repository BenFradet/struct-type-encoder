lazy val buildSettings = Seq(
  organization := "com.github.benfradet",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val shapelessVersion = "2.3.2"
lazy val sparkVersion = "2.1.0"

lazy val structTypeEncoder = (project in file("."))
  .settings(name := "struct-type-encoder")
  .settings(buildSettings)
  .settings(
    initialCommands in console :=
      """
        |import benfradet.ste._
        |import benfradet.ste.StructTypeEncoder._
      """.stripMargin
  )
  .settings(libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
  ))