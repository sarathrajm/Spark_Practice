name := "scala_spark"

version := "0.1"

scalaVersion := "2.11.8"


//libraryDependencies ++= Seq(
  //"org.apache.spark" % "spark-core_2.11" % "2.3.0",
  //"org.apache.spark" % "spark-sql_2.11" % "2.3.0"

   //"org.apache.spark" %% "spark-core" % "2.0.0",
   //"org.apache.spark" %% "spark-sql" % "2.2.1"

//)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

