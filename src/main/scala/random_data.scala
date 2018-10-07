import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object random_data {

  def main(args: Array[String]) : Unit ={

    def generate_line() : String = {

      val r = new Random().nextDouble()

      r match {

        case x if (x > 0 && x < 0.1) => "Error something went wrong"
        case x if (x > 0.1 && x < 0.3) => "Warn this is warning"
        case x if (x > 0.3 && x < 0.7) => "Info this is Information"
      }

    }

    val conf = new SparkConf().setAppName("testGroup").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    sc.parallelize(1 to 1000,3).map(l => generate_line()).saveAsTextFile("/user/sarathrajm/test_file.txt")

  }
}
