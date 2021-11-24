import org.apache.spark.sql.SparkSession

class combinedtable(spark:SparkSession) {

  def createTable(): Unit ={

    spark.sql("drop table if exists lifeexpectpharmspending")
    spark.sql("create table lifeexpectpharmspending as select lifeexpect.country,lifeexpect.year,status,adult_mortality,lifeexpect.gdp,pc_healthxp,total_spend,exports_percentGDP,imports_percentGDP,inflation_GDP_deflator,population_growth,total_population,gdp_growth,life_expectancy from lifeexpect join countrycode on LifeExpect.Country=countryCode.Country join pharmspending on countryCode.country_code=pharmspending.country_code and lifeexpect.year=pharmspending.time join gdp on lifeexpect.country=gdp.country_name and lifeexpect.year=gdp.year").cache()
    spark.sql("select * from lifeexpectpharmspending").show(100)

  }
}