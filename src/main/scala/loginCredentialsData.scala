import org.apache.spark.sql.SparkSession

class loginCredentialsData(spark:SparkSession) {
  def createTable() {
    spark.sql("DROP TABLE IF EXISTS loginDB")
    spark.sql("CREATE TABLE IF NOT EXISTS loginDB(username varchar(255) ,password varchar(255),role varchar(255)) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'Inputs/loginCredentials.txt' INTO TABLE loginDB")
  }
}
