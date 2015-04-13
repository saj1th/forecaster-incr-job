lazy val root = (project in file(".")).
  settings(
    name := "forecaster-incr-job",
    version := "0.5",
    scalaVersion := "2.10.4"
  )



// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.2.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-rc3",
  "spark.jobserver" % "job-server-api" % "0.5.0" % "provided"
)

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "Sonatype OSS Repo" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype OSS Snapshots Repo" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype OSS Tools Repo" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
)

//discarding META-INF for assembly
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
