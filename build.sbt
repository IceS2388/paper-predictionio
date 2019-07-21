name := "paper-predictionio"

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.14.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.1.3" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided",
  "org.deeplearning4j" %% "dl4j-spark" % "1.0.0-beta4_spark_2",
  "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta4",
  "org.nd4j" % "nd4j-native-platform" % "1.0.0-beta4",
  "org.datavec" % "datavec-api" % "1.0.0-beta4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.7.9",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.7.9"
)


