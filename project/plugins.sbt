resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.4.0")

// === dependecy graph
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.0")

resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.7.0-SNAPSHOT")