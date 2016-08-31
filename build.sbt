name := "spark-streaming-mqtts"
organization := "co.verdigris"
version := "0.1.0"
scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.6", "2.11.8")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided"
libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.1"
libraryDependencies += "co.verdigris.security.specs" % "spec-pkcs1" % "0.1.0"

resolvers += "Eclipse MQTT Snapshots" at "http://sofia2.org/nexus/content/groups/public"
resolvers += "Verdigris Security Specs" at "https://verdigristech.github.io/spec-pkcs1/"

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}