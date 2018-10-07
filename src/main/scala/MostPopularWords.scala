import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext

object MostPopularWords {

  def main(args: Array[String]) {

    // Set the log level to only print errors

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext

    val conf = new SparkConf().setAppName("MostPopularWords").set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext()

    // Read each line of the book into an RDD

    val lines = sc.textFile(args(0)) // RDD CREATED

    // Split into words separated by a space character

    val words = lines.flatMap(x => x.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    // Save the file into HDFS.

    words.saveAsTextFile(args(1))

  }

}
