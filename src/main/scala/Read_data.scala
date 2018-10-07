import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Read_data {

  val conf = new SparkConf().setMaster("local").setAppName("Chicago crime data analysis")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val df1 = sc.textFile("/user/sarathrajm/Crimes_-_2001_to_present.csv")
    val header = df1.first()
    val df2 = df1.filter(x => x != header)
    df2.saveAsTextFile("/user/sarathrajm/Crimes_data_withoutheader.csv")


    val df3 = sc.textFile("/user/sarathrajm/chicago-community-areas.csv")
    val header1 = df3.first()
    val df4 = df3.filter(x => x!= header1)
    df4.saveAsTextFile("/user/sarathrajm/chicago_commnity_no_header.csv")


  }
}
