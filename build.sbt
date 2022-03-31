name := "SparkMLCB"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"


resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

// for catboost
val sparkCompatVersion:String = "3.0"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",


  // Alon: In case we need support for S3/Hadoop store
  //"org.apache.hadoop" % "hadoop-common" % "3.0.0",
  //"org.apache.hadoop" % "hadoop-client" % "3.0.0",
  //"org.apache.hadoop" % "hadoop-aws" % "3.0.0"

  //JSON library

   "com.typesafe.play" %% "play-json" % "2.9.2",

  "ai.catboost" % "catboost-spark_3.1_2.12" % "1.0.4",
  "org.apache.spark" %% "spark-mllib" % "3.1.2" ,
//"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2"

)






