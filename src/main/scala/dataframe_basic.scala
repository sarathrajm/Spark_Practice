

import org.apache.spark.sql.SparkSession



object dataframe_basic {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)
  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().appName("Dataframe_basic").getOrCreate()

    import spark.implicits._

    val custs = Seq(

      Cust(1,"sarath",1000.00,10.0,"CA"),
      Cust(2,"madhura",2000.00,10.0,"CA"),
      Cust(3,"Rajesh",3000000.00,10.00,"CA"),
      Cust(4, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(5, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(6, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val custDF = {
      spark.sparkContext.parallelize(custs, 4).toDF()
    }


    println("*** toString() just gives you the schema")
    println(custDF.toString())
    println("*** It's better to use printSchema()")
    custDF.printSchema()
    println("*** show() gives you neatly formatted data")
    custDF.show()
    println("**********use select to choose one column")
    custDF.select("id").show()
    println("*** use select() for multiple columns")
    custDF.select("name","sales").show()
    println("*** use filter() to choose rows")
    custDF.filter($"state".equalTo("CA")).show()

  }
}
