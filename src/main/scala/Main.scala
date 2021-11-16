import org.apache.spark.{SparkConf, SparkContext}
import java.util.{InputMismatchException, Scanner}
//import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

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
    //spark.sql("SELECT * FROM loginDB").show()

    def login(): String = { //Prompts the user to log in. If their data matches a record in the loginDB they are logged in.
      print("Username: ")
      var user = scanner.nextLine()
      print("Password: ")
      var password = scanner.nextLine()
      val userDF = spark.sql("SELECT * FROM loginDB WHERE username = '"+ user +"' AND password = '"+password+"';")
      if (userDF.count() == 1){
        println("Logging in as " + user)
      } else {
        println("ERROR: (Incorrect Login credentials)")
      }
      return user
    }

    def createUser():String ={ //Prompts the user to create an account. Checks the table for conflicts in username and inserts new user if none are found.
      print("Desired username: ")
      var user = scanner.nextLine()
      if(spark.sql("SELECT * FROM loginDB WHERE username ='"+user+"';").count() >0){
        println("ERROR: (Username already in use)")
        return ""
      } else {
        print("Desired Password: ")
        val password = scanner.nextLine()
        print("Confirm Password: ")
        val passwordConfirm = scanner.nextLine()
        if(password==passwordConfirm){
          spark.sql("INSERT INTO loginDB(username,password,role) VALUES('"+user+"','"+password+"','USER')")
          //spark.sql("SELECT * FROM loginDB").show()
          println("User created successfully... logging in as "+user)
          return user
        } else {
          println("ERROR: (Password mismatch)")
          return ""
        }
      }
      return ""
    }

    var currentUser =  ""
    var userRole = ""


    while(true){
      println("Please select 1 to log in or 2 to create an account. 0 to exit.")
      var i = scanner.nextLine()
      i match {
        case "1" =>
          currentUser = login() //Attempt to log in
        case "2" =>
          currentUser = createUser() //Attempt to create a user and log in
        case "0" =>
          println("Exiting...") //Exit program and close spark
          spark.stop()
          return
        case default =>
          println("ERROR: (Incorrect Input)") //In the event of incorrect input
      }
    }



    spark.stop()
  }
}
