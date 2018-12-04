organization := "org.daron"

name := "cats-effect-exercises"

version := "0.0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "1.0.0"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-language:experimental.macros",
  "-language:higherKinds",
  "-language:postfixOps",
  "-feature",
  "-unchecked",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ypartial-unification"
)
