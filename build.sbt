import SonatypeKeys._

filterScalaLibrary := false

import scalariform.formatter.preferences.{SpacesAroundMultiImports, CompactControlReadability, PreserveSpaceBeforeArguments, DoubleIndentClassDeclaration}

name := "reactive-kafka"

val akkaVersion = "2.4.1"
val akkaStreamVersion = "2.0"
val curatorVersion = "2.9.0"

val kafka = "org.apache.kafka" %% "kafka" % "0.9.0.0" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j")
val curator = Seq("org.apache.curator" % "curator-framework" % curatorVersion,
  "org.apache.curator" % "curator-recipes" % curatorVersion
)

val commonDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)

val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamVersion excludeAll (ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.11")),
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  kafka,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.reactivestreams" % "reactive-streams-tck" % "1.0.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit-experimental" % akkaStreamVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test"
)

val zkCommitterDependencies = Seq(kafka) ++ curator

val commonSettings = scalariformSettings ++ Seq(
  version := "0.9.0-SNAPSHOT",
  organization := "com.softwaremill.reactivekafka",
  startYear := Some(2014),
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  homepage := Some(url("https://github.com/softwaremill/reactive-kafka")),
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8", // yes, this is 2 args
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture"
  ),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(PreserveSpaceBeforeArguments, true)
    .setPreference(CompactControlReadability, true)
    .setPreference(SpacesAroundMultiImports, false))

val publishSettings = Seq(
  publishMavenStyle := false,

  publishArtifact in ThisBuild := true,

  publishTo := Some {
    if (isSnapshot.value)
      Resolver.url("Qordoba snapshots", url("http://master.qordobadev.com:8088/artifactory/qordoba-snapshots"))(Resolver.ivyStylePatterns)
    else
      Resolver.url("Qordoba releases", url("http://master.qordobadev.com:8088/artifactory/qordoba-releases"))(Resolver.ivyStylePatterns)
  },

  credentials += Credentials(Path.userHome / ".ivy2" / ".qordobaArtifactoryDeployerCredentials")
)

lazy val root =
  project.in(file("."))
    .settings(commonSettings)
    .settings(Seq(
      publishArtifact := false,
      publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))))
    .aggregate(zookeeperCommitter, core)

lazy val core = project
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(Seq(
    name := "reactive-kafka-core",
    libraryDependencies ++= commonDependencies ++ coreDependencies
  ))

lazy val zookeeperCommitter = Project(
  id = "zookeeper-committer",
  base = file("./zookeeper-committer")
)
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(libraryDependencies ++= commonDependencies ++ zkCommitterDependencies)
  .dependsOn(core % "compile->compile;test->test")
