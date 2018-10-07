import org.apache.spark.{SparkContext,SparkConf}

object analysis_data {

  val conf = new SparkConf().setAppName("Chicago Crime Data Analysis").setMaster("local")
  val sc = new SparkContext(conf)


  def main(args: Array[String]): Unit = {

    val crimewithnohdr  = sc.textFile("/user/sarathrajm/Crimes_data_withoutheader.csv")
    val commwithnohdr   = sc.textFile("/user/sarathrajm/chicago_commnity_no_header.csv")

    val accum_blank = sc.longAccumulator("Blank community")
    var accum2 : Int = 0
    val b= crimewithnohdr.map(x => {

      if(x.split(",")(13)=="")
        accum_blank.add(1)
        accum2+= 1
    })

    b.count()
    println("accumulator value:" + accum_blank.value)
    println("normal value :" + accum2)

    val crimewithcomm = crimewithnohdr.filter( x => {x.split(",")(13) != "" || x.split(",")(13) != 0})
    val crimefinal= crimewithcomm.map(x => (x.split(",")(13),1)).reduceByKey(_+_)
    val commfinal=commwithnohdr.map(x => (x.split(",")(0),x.split(",")(1)))

    /** Most crimes: */
      crimefinal.join(commfinal).map(x => (x._2._1.toInt,(x._1,x._2._2))).takeOrdered(10)(Ordering[Int].reverse.on(x =>x._1)).foreach(println)


    /** Least Crimes: */
      crimefinal.join(commfinal).map(x => (x._2._1.toInt,(x._1,x._2._2))).takeOrdered(10)(Ordering[Int].on(x =>x._1)).foreach(println)


  }

}
