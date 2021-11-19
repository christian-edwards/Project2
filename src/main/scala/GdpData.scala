import org.apache.spark.sql.SparkSession

class GdpData(spark:SparkSession) {

  //functions will create a spark sql table
  def createTable() = {

    //gets the file from resources folder
    val filePath="src/main/resources/gpdTranspose.csv"
    //val filePath="Baby_Names__Beginning_2007.csv"

    //creates a dataframe
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
    df2.show(false)
    df2.printSchema()

    //makes a temporary table
    df2.createOrReplaceTempView("gdpDF")
    val sqlDF = spark.sql("SELECT * FROM gdpDF")
    sqlDF.show()


    spark.sql( " DROP TABLE IF  EXISTS gdp")

    //write to warehouse
    val a = spark.sql("create table gdp as select * from gdpDF")

  }




//TODO
//TODO print a query out
//TODO Graph by year x year y the rise for any country we want
//TODO Queries in sql

}
