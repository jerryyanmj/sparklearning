name := "sparklearning"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "opennlp sourceforge repo" at "http://opennlp.sourceforge.net/maven2"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/"