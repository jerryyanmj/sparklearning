name := "sparklearning"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.github.nscala-time" %% "nscala-time" % "2.0.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.commons" % "commons-pool2" % "2.4.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.avro" % "avro" % "1.7.7" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.avro" % "avro-mapred" % "1.7.7" excludeAll ExclusionRule(organization = "javax.servlet")

)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "opennlp sourceforge repo" at "http://opennlp.sourceforge.net/maven2"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/"