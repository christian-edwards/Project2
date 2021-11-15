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

    spark.sql("CREATE DATABASE IF NOT EXISTS project2DB")
    spark.sql("USE project2DB")
    spark.sql("DROP TABLE IF EXISTS loginDB")
    spark.sql("CREATE TABLE IF NOT EXISTS loginDB(username varchar(255) ,password varchar(255),role varchar(255)) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'Inputs/loginCredentials.txt' INTO TABLE loginDB")
    spark.sql("SELECT * FROM loginDB").show()



    spark.stop()
  }
}
