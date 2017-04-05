name := "spark-sandbox"

scalaVersion := "2.11.8"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.kudu" %% "kudu-spark2" % "1.2.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3", // excludeAll(excludes:_*),
  "mysql" % "mysql-connector-java" % "5.1.29",
  "org.postgresql" % "postgresql" % "9.4-1201-jdbc4",
  "oracle" % "ojdbc6" % "11.2.0.3.0",
  "com.zaxxer" % "HikariCP" % "2.5.1",
  "com.typesafe.play" %% "anorm" % "2.4.0",
  "com.typesafe" % "config" % "1.3.1"
)

fork in runMain := true

javaOptions in runMain ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

outputStrategy := Some(StdoutOutput)

import ReleaseTransformations._
import sbtrelease.Version.Bump.Bugfix

releaseTagName := s"${if (releaseUseGlobalVersion.value) (version in ThisBuild).value else version.value}"
releaseVersionBump := Bugfix

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
//  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  //publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)