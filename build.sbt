import AssemblyKeys._ // put this at the top of the file

name := "learningstorm"

scalaVersion := "2.9.1"

resolvers += "Clojars" at "http://clojars.org/repo"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "storm" % "storm" % "0.8.0" % "provided"

libraryDependencies += "com.github.velvia" %% "scala-storm" % "0.2.2-SNAPSHOT"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.0.6"

libraryDependencies += "com.google.guava" % "guava" % "13.0.1"

libraryDependencies += "org.jsoup" % "jsoup" % "1.6.3"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "6.0.4"

libraryDependencies += "redis.clients" % "jedis" % "2.1.0"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.1"

assemblySettings

mainClass in assembly := Some("WabbitTopology")

net.virtualvoid.sbt.graph.Plugin.graphSettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("project.clj") => MergeStrategy.first
    case x => old(x)
  }
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "storm-0.8.0.jar"}
}
