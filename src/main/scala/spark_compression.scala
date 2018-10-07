import org.apache.spark.sql.SparkSession


object spark_compression {

  case class actor(name: String, actor: String, episodeDebut: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("spark handling cpmoression").master("local").
      config("spark.sql.warehouse.dir", "file:///C:/Users/madhu/IdeaProjects/scala_spark/spark-warehouse").getOrCreate()


    val df = spark.createDataFrame(
      actor("Homer", "Dan Castellaneta", "Good Night") ::
        actor("Marge", "Julie Kavner", "Good Night") ::
        actor("Bart", "Nancy Cartwright", "Good Night") ::
        actor("Lisa", "Yeardley Smith", "Good Night") ::
        actor("Maggie", "Liz Georges and more", "Good Night") ::
        actor("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
        Nil).toDF().cache()

    df.show();

    df.write.mode("overwrite").format("parquet").option("compression" , "none").save("/user/sarathrajm/parquet")
    df.write.mode("overwrite").format("parquet").option("compression", "none").mode("overwrite").save("/user/sarathrajm/parquet1")
  }


}
