name := "sparklearning"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
  "com.github.nscala-time" %% "nscala-time" % "2.0.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.commons" % "commons-pool2" % "2.4.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.avro" % "avro" % "1.7.7" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.avro" % "avro-mapred" % "1.7.7" excludeAll ExclusionRule(organization = "javax.servlet")
).map(_.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),ExclusionRule(organization = "org.scalatest")))

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/",
  "opennlp sourceforge repo" at "http://opennlp.sourceforge.net/maven2",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")