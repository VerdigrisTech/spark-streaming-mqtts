name := "spark-streaming-mqtts"
organization := "co.verdigris"
version := "0.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.1"
libraryDependencies += "co.verdigris.security.specs" % "spec-pkcs1" % "0.1.0"

resolvers += "Eclipse MQTT Snapshots" at "http://sofia2.org/nexus/content/groups/public"
resolvers += "Verdigris Security Specs" at "https://verdigristech.github.io/spec-pkcs1/"