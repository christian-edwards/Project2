import org.apache.spark.sql.SparkSession

class PharmSpendingData(spark:SparkSession) {

  //functions will create a spark sql table
  def createTable() = {

    //gets the file from resources folder
    val filePath="src/main/resources/PharmSpending.csv"

    //creates a dataframe
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
    df2.show(false)
    df2.printSchema()

    //makes a temporary table
    df2.createOrReplaceTempView("PharmSpendingDF")
    val sqlDF = spark.sql("SELECT * FROM PharmSpendingDF")
    sqlDF.show(10)


    spark.sql( " DROP TABLE IF EXISTS PharmSpending")

    //write to warehouse
    val a = spark.sql("create table PharmSpending as select * from PharmSpendingDF")

  }

}