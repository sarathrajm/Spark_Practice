import java.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object groupByTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("testGroup").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val nummappers =100
    val numkvpairs=10000
    val valsize=1000
    val numreducers=36
    val a = sc.textFile("/user/sarathrajm/sparkproperties.txt")
    val pairs1=a.map(x => (x.split(",")(0),1)).repartition(5).cache()
    /*val pairs1 = sc.parallelize(0 until 100, 100).flatMap { p =>
      //val rangen = new Random
      var arr1= new Array[(Int, Array[Byte])](numkvpairs)
      for(i <- 0 until numkvpairs) {
        val bytearr = new Array[Byte](valsize)
        rangen.nextBytes(bytearr)
        arr1 (i)=(rangen.nextInt(Int.MaxValue),bytearr)
      }
      arr1
    }.cache*/
    pairs1.count
    println(pairs1.groupByKey.count)
    sc.stop()
  }

}
