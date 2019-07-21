name := "paper-predictionio"

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.14.0",
  "org.apache.spark" %% "spark-core" % "2.1.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "org.deeplearning4j" % "deeplearning4j-core",
  "org.datavec" % "datavec-api" % "1.0.0-beta4",
  "org.nd4j" % "nd4j-native-platform" % "1.0.0-beta4",
  "org.nd4j" % "nd4j-api" % "1.0.0-beta4"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "datavec", xs@_*) => MergeStrategy.last
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
