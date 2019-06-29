name := "paper-predictionio"

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
"org.apache.predictionio" %% "apache-predictionio-core" % "0.14.0" % "provided",
  "org.apache.spark"        %% "spark-mllib"              % "2.4.0" % "provided")

// https://mvnrepository.com/artifact/org.apache.mahout/mahout-core
libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"


// https://mvnrepository.com/artifact/org.apache.mahout/mahout-math
libraryDependencies += "org.apache.mahout" % "mahout-math" % "0.9"
