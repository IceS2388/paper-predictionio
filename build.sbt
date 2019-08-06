name := "paper-predictionio"

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.14.0",
  "org.apache.spark" %% "spark-core" % "2.1.3",
  "org.apache.spark" %% "spark-sql" % "2.1.3",
  "org.apache.spark" %% "spark-mllib" % "2.1.3"
)


