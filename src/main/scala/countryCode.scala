import org.apache.spark.sql.SparkSession

class countryCode(spark:SparkSession) {

  //functions will create a spark sql table
  def createTable() = {


    //gets the file from resources folder
    val filePath="src/main/resources/countryCode.csv"

    //creates a dataframe
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
    df2.show(false)
    df2.printSchema()

    //makes a temporary table
    df2.createOrReplaceTempView("countryCodeDF")
    val sqlDF = spark.sql("SELECT * FROM countryCodeDF")
    sqlDF.show(10)

    spark.sql( " DROP TABLE IF EXISTS countryCode")

    //write to warehouse
    val a = spark.sql("create table countryCode as select * from countryCodeDF")

  }

}