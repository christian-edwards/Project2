import org.apache.spark.{SparkConf, SparkContext}
import java.util.{InputMismatchException, Scanner}
//import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {






  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")//TODO Edit this to work on your PC.
    val scanner = new Scanner(System.in)
    //Logger.getLogger("org").setLevel(Level.OFF)//TODO Uncomment these to suppress INFO in output.
    //Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .appName("Project2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //val countryCodeTable = new countryCode(spark)
    //val GDPDataTable = new GdpData(spark)
    //val LifeExpectData = new LifeExpectData(spark)
    val loginCredentialsTable = new loginCredentialsData(spark)

    spark.sql("CREATE DATABASE IF NOT EXISTS project2DB")
    spark.sql("USE project2DB")

    loginCredentialsTable.createTable()
    //countryCodeTable.createTable()
    //GDPDataTable.createTable()
    //LifeExpectData.createTable()


    def adminMenu(): Unit ={//TODO: Add later, this will be a menu you can access as an ADMIN from the loginMenu

    }

    def loginMenu(username:String,role:String): Unit ={//The menu the user has when logged in.
      while(true){
        println(role+": "+username)
        println("Please select 1 to TEST or 2 to TEST. 0 to log out.")
        var j = scanner.nextLine()
        j match {
          case "1" =>
            println("Test 1")
          case "2" =>
            println("Test 2")
          case "0" =>
            println("Logging out...\n")
            return
          case default =>
            println("ERROR: (Incorrect Input)")
        }
      }
    }

    def login(): String = { //Prompts the user to log in. If their data matches a record in the loginDB they are logged in.
      print("Username: ")
      var user = scanner.nextLine()
      print("Password: ")
      var password = scanner.nextLine()
      val userDF = spark.sql("SELECT * FROM loginDB WHERE username = '"+ user +"' AND password = '"+password+"';")
      if (userDF.count() == 1){
        val roles = userDF.select("role").collect().map(_.getString(0)).mkString("")
        if(roles == "ADMIN"){
          println("Logging in as ADMIN: " + user)
          loginMenu(user,roles)
        } else {
          println("Logging in as USER: " + user)
          loginMenu(user,roles)
        }

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
          println("User created successfully... logging in as USER: "+user)
          loginMenu(user,"USER")
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
          login() //Attempt to log in
        case "2" =>
          createUser() //Attempt to create a user and log in
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
