import org.apache.spark.sql.SparkSession
import java.util.{InputMismatchException, Scanner}


class loginCredentialsData(spark:SparkSession) {
  def createTable() {
    //spark.sql("DROP TABLE IF EXISTS loginDB")
    spark.sql("CREATE TABLE IF NOT EXISTS loginDB(username varchar(255) ,password varchar(255),role varchar(255)) row format delimited fields terminated by ','")
    if(spark.sql("SELECT * FROM loginDB").count ==0 ){spark.sql("LOAD DATA LOCAL INPATH 'Inputs/loginCredentials.txt' INTO TABLE loginDB")}
  }

  def insert(user:String,password:String,role:String): Unit ={
    spark.sql("INSERT INTO loginDB(username,password,role) VALUES('"+user+"','"+password+"','"+role+"')")
  }


  def changeRoll(): Unit ={
    val input = new Scanner(System.in)
    print("Enter a username to change roll: ")
    val user = input.nextLine()
    val userDF = spark.sql("SELECT * FROM loginDB WHERE username = '"+ user +"';")
    if (userDF.count() == 1){
      print("Enter new roll for username (ADMIN or USER): ")
      val role = input.nextLine()
      if(role == "ADMIN" || role == "USER"){spark.sql("UPDATE loginDB SET role = '"+role+"' WHERE username ='"+user+"'")
      } else {println("ERROR: (Incorrect Role)")}
    } else {println("ERROR: (User not found)")}
  }


}
