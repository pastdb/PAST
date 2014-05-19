name := "past"

version := "0.1"

scalaVersion := "2.10.3"

resolvers ++= Seq(
  "Sonatype OSS Releases"  at "http://oss.sonatype.org/content/repositories/releases/",
  "Akka repository" at "http://repo.akka.io/releases"
)

libraryDependencies ++= Seq(
  "nz.ac.waikato.cms.weka" % "weka-stable" % "3.6.6",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1", // for hdfs and fs abstractions
  "com.typesafe" % "config" % "0.4.0", // configuration library
  "net.ceedubs" %% "ficus" % "1.0.0", // scala wrapper for config
  "org.apache.spark" % "spark-mllib_2.10" % "0.9.1",
  "org.apache.spark" % "spark-core_2.10" % "0.9.1"
)

javacOptions ++= Seq(
  "-encoding", "UTF-8"
)

// needed to enable java assert in Test
fork in Test := true

// enable java assert in Test
javaOptions in Test += "-enableassertions"

