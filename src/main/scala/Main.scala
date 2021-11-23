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

    val countryCodeTable = new countryCode(spark)
    val GDPDataTable = new GdpData(spark)
    val LifeExpectData = new LifeExpectData(spark)
    val loginCredentialsTable = new loginCredentialsData(spark)
    val pharmSpendingDataTable = new  PharmSpendingData(spark)

    val combinedTableData = new combinedtable(spark)


    spark.sql("CREATE DATABASE IF NOT EXISTS project2DB")
    spark.sql("USE project2DB")

    loginCredentialsTable.createTable()
    countryCodeTable.createTable()
    GDPDataTable.createTable()
    LifeExpectData.createTable()
    pharmSpendingDataTable.createTable()
    combinedTableData.createTable()

    val queries = new Queries(spark)

    def adminMenu(): Unit ={//TODO: Add later, this will be a menu you can access as an ADMIN from the loginMenu
      while(true){
        println("ADMIN CONSOLE: Please select 1 - 3 to view ADMIN commands or 0 to return to USER menu.")
        var m = scanner.nextLine()
        m match {
          case "1" =>
          spark.sql("SELECT username,role FROM loginDB").show()
          case "2" =>
            println("Making a new ADMIN...")
            createUser("ADMIN")
          case "3" =>
            println("Admin Test 3")
          case "0" =>
            println("Returning to user menu...")
            return
          case default => println("ERROR: (Incorrect input)")
        }
      }
    }

    def loginMenu(username:String,role:String): Unit ={//The menu the user has when logged in.
      while(true){
        println(role+": "+username)
        println("Please select 1 - 6 to select a query or 0 to log out.")
        if(role == "ADMIN"){println("Select 7 to open ADMIN console.")}
        var j = scanner.nextLine()
        j match {
          case "1" =>
            println("Query 1: What is the country status, mortality rate and life expectancy of all countries?")
            queries.query1()
          case "2" =>
            println("Query 2: What are some relevant details about a selected country?")
            queries.query2()
          case "3" =>
            println("Query 3: What is the GDP vs Healthcare expenditure per country?")
            queries.query3()
          case "4" =>
            println("Query 4: What country has the highest Healthcare expenditure compared to life expectancy and mortality rate?")
            queries.query4()
          case "5" =>
            println("Query 5: Average Healthcare across all countries based on development status?")
            queries.query5()
          case "6" =>
            println("Query 6: Do countries with a higher GDP spend more in pharmaceuticals for their population?")
            queries.query6()
          case "7" =>
            if(role =="ADMIN"){adminMenu()}
            else{println("ERROR: (Incorrect Input)")}
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

    def createUser(role:String):String ={ //Prompts the user to create an account. Checks the table for conflicts in username and inserts new user if none are found.
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
          loginCredentialsTable.insert(user,password,role)
          if(role == "USER"){
            println(role+" created successfully... logging in as "+user)
            loginMenu(user,role)
          } else {
            println("ADMIN "+user+" created successfully.")
          }

          return user
        } else {
          println("ERROR: (Password mismatch)")
          return ""
        }
      }
      return ""
    }

    spark.sql("SELECT username,role FROM loginDB").show()

    var currentUser =  ""
    var userRole = ""

    while(true){
      println("Please select 1 to log in or 2 to create an account. 0 to exit.")
      var i = scanner.nextLine()
      i match {
        case "1" =>
          login() //Attempt to log in
        case "2" =>
          createUser("USER") //Attempt to create a user and log in
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
