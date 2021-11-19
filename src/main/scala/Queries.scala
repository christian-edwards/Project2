import org.apache.spark.sql.SparkSession

class Queries(spark:SparkSession) {

  def runQueries(): Unit ={
    spark.sql("select * from gdp").show(5)
    spark.sql("select * from LifeExpect").show(5)
    spark.sql("select * from PharmSpending").show(5)
    spark.sql("select * from countryCode").show(5)
  }
}
