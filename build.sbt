import Dependencies._
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "kafka-eval",
    libraryDependencies += "com.typesafe" % "config" % "1.3.3",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0",
    libraryDependencies += "org.jsoup" % "jsoup" % "1.11.3",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.14",
    libraryDependencies += "com.iheart" %% "ficus" % "1.4.3",
    libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.5.14" % Test,
    libraryDependencies += scalaTest % Test
  )
