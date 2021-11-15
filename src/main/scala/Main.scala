import org.apache.spark.{SparkConf, SparkContext}
import java.util.{InputMismatchException, Scanner}
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")//Edit this to work on your PC.
    val scanner = new Scanner(System.in)
    val spark = SparkSession
      .builder
      .appName("Project2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")







    spark.stop()
  }
}
