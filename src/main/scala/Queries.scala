import org.apache.spark.sql.SparkSession
import java.util.{InputMismatchException, Scanner}


class Queries(spark:SparkSession) {

  val scanner = new Scanner(System.in)

  def selectAll(): Unit ={
    spark.sql("select * from gdp").show(5)
    spark.sql("select * from LifeExpect").show(5)
    spark.sql("select * from PharmSpending").show(5)
    spark.sql("select * from countryCode").show(5)
  }

  def query1(): Unit ={
    spark.sql("SELECT country,year,adult_mortality,life_expectancy FROM lifeexpectpharmspending").show()
    //spark.sql("select country,status,avg(gdp) as avg_gdp,avg(gdp_growth) as avg_gdp_growth, avg(pc_healthxp) as avg_pharm_spend,sum(total_spend) as healthcare_spend from lifeexpectpharmspending group by country,status order by country").show()
  }

  def query2(): Unit ={
      print("Select a country: ")
      val country = scanner.nextLine()
      spark.sql("SELECT country,year,adult_mortality,life_expectancy FROM lifeexpectpharmspending WHERE country = '"+country+"'").show()
  }

  def query3(): Unit ={
    spark.sql("SELECT country,gdp,adult_mortality,pc_healthxp FROM lifeexpectpharmspending").show()
  }

  def query4(): Unit ={
    spark.sql("SELECT country,pc_healthxp,life_expectancy,adult_mortality FROM lifeexpectpharmspending ORDER BY pc_healthxp DESC").show()
  }

  def query5(): Unit ={
    spark.sql("SELECT status, AVG(pc_healthxp) AS Average_Expenditure, AVG(adult_mortality) AS Average_Adult_Mortality FROM lifeexpectpharmspending GROUP BY status").show()
  }

  def query6(): Unit ={
    spark.sql("select country,status,round(avg_gdp,2) as avg_gdp,round(avg_gdp_growth,2) as avg_gdp_growth, round(avg_pharm_spend,2) as avg_pharm_spending,round(healthcare_spend,2) as total_healthcare_spending from (select country,status,avg(gdp) as avg_gdp,avg(gdp_growth) as avg_gdp_growth, avg(pc_healthxp) as avg_pharm_spend,sum(total_spend) as healthcare_spend from lifeexpectpharmspending group by country,status) order by country").show()
  }
}
