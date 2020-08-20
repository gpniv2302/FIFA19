name := "FIFA19"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.11"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1"


dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}