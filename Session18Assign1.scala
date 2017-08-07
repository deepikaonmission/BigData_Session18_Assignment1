/*
<<<<<<<<<<------------------------- QUERIES ----------------------->>>>>>>>>>>
1) What is the distribution of the total number of air-travelers per year
2) What is the total air distance covered by each user per year
3) Which user has travelled the largest distance till date
4) What is the most preferred destination for all users.

 */

import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}

/* Explanation of above line ------>>>>>>>>
-> SparkSession:
* SparkSession is the entry point to Spark SQL.
* It is the very first object which is created while developing Spark SQL applications using the fully-typed Dataset (or untyped Row-based DataFrame) data abstractions.
* Using SparkSession sqlContext and sparkContext can be accessed in the same application in spark 2.0
* Note:	SparkSession has merged SQLContext and HiveContext in one object in Spark 2.0.

-> SQLContext:
* The entry point into all functionality in Spark SQL is the SQLContext class or one of its descendants.

-> Row:
* Interface that belongs to org.apache.spark.sql package
* Row is also known as Catalyst Row
* Import is required to work with Row objects

-> Column:
* Class that belongs to org.apache.spark.sql package
* import is required to access and to work with columns in a DataFrame/DataSet
*/

object Session18Assign1 extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session18Assign1Submit")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment1/DataSet")
    .getOrCreate()
  /* Explanation of above SparkSession statement
  -> builder creates a new Builder that is used to build a fully-configured SparkSession using a fluent API
  -> master sets Master URL which is local in this case,
  -> appName sets Application name here application name is "Sesison18Assign1Submit" which can be helpful in case of debugging
  -> config sets directory path of dataSource/warehouse from where source files will be accessed, below is the syntax of config
     config("spark.some.config.option", "some-value")
  -> SparkSession.builder().getOrCreate(): it instantiates a SparkSession object if one does not already exist, along with its associated underlying contexts.
  */

  val df = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment1/DataSet/S18_Dataset_Holidays.txt")
  /*Explanation of above line
  -> to work with spark sql sqlContext is used
  -> here df variable with type sql.DataFrame is created by reading csv file from the specified location
  */

  df.show()         //to show the data inside df in tabular format show() method is used
  //REFER Screenshot 1 for output

  import spark.implicits._         //to work with case class this import is required just before case class
  case class holidaysClass(user_id:Int, src:String, dest:String, travel_mode:String, distance:Int, year_of_travel:Int)
  /* Explanation of above line
  -> case class "holidaysClass" is created, in order to
     * provide descriptive name to columns
     * infer schema (i.e. which are numeric columns, string columns etc.)
   */

  val df1 = df.map{
    case Row (
    a:String,
    b:String,
    c:String,
    d:String,
    e:String,
    f:String) => holidaysClass(user_id=a.toInt,src=b,dest=c,travel_mode=d,distance=e.toInt,year_of_travel=f.toInt)
  }
  /* Explanation of above line
  -> df1 -->> Dataset[Session18Assign1.holidaysClass] is created
  -> where df is mapped with case class
  */

  df1.show()     //data inside df1 is shown in tabular form
  //REFER Screenshot 2 for output

  df1.createOrReplaceTempView("holidaysTable")      //temporary table holidaysTable is created from df1

  spark.sql("select * from holidaysTable").show()      //using spark.sql, sql queries can be worked with,
                                                  // here holidaysTable data is fetched using select query,
                                                  // and fetched data is shown using show() method in tabular form
  //REFER Screenshot 3 for output

  //<<<<<<<----------------------  QUERY 1 -------------------------->>>>>>>>
  //1. What is the distribution of the total number of air-travelers per year

  spark.sql("select year_of_travel as year,count(*) as distribution from holidaysTable group by year_of_travel order by year_of_travel").show()

  /* Explanation ----->>>>>
   -> here grouping is done on the basis of "year_of_travel" column
   -> and count is done along with year so total count of air-travllers per year can be retreived
   -> finally order by year_of_travel is done to show the data in ascending order, year wise
   */

  //REFER Screenshot 4 for output

  //<<<<<<<----------------------  QUERY 2 -------------------------->>>>>>>>
  //2. What is the total air distance covered by each user per year

  spark.sql("select year_of_travel as year,user_id as user,sum(distance) as totalDistance from holidaysTable group by year_of_travel,user_id order by year_of_travel,user_id").show()

  /* Explanation ----->>>>>
   -> here grouping is done on the basis of two columns i.e. "year_of_travel" and user_id
   -> and sum of distance is computed along with year and user so total air distance covered by each user per year can be retrieved
  */

  //REFER Screenshot 5 for output

  //<<<<<<<----------------------  QUERY 3 -------------------------->>>>>>>>
  //3. Which user has travelled the largest distance till date

  spark.sql("select user_id as user,sum(distance) as largestDistance from holidaysTable" +
    " group by user_id" +
    " having sum(distance) IN (select max(sumOfDistance) from" +
    " (select sum(distance) as sumOfDistance from holidaysTable group by user_id))").show()

  /* Explanation ----->>>>>
   -> nested queries/sub-queries are used in order to process asked query
   -> subquery : (select sum(distance) as sumOfDistance from holidaysTable group by user_id)
      it calculates sum of distance travelled by each user
   -> subquery : (select max(sumOfDistance) from" +
    " (select sum(distance) as sumOfDistance from holidaysTable group by user_id))
     outer query receives the data from inline view i.e. query placed in from clause and calculates the max of
     the sum of distance returned by inline view
   -> main query displays user_id and calculates the sum of distance per user and using having clause filters
      only those sum(distance) which match max of sum of distance
  */

  //REFER Screenshot 6 for output

  //<<<<<<<----------------------  QUERY 4 -------------------------->>>>>>>>
  //4. What is the most preferred destination for all users

  val df2 = spark.sql("select dest, count(user_id) as countOfUsers from holidaysTable group by dest")
  df2.createOrReplaceTempView("table1")
  val df3 = spark.sql("select max(countOfUsers) as maxTravellers from table1")
  df3.createOrReplaceTempView("table2")
  spark.sql("select table1.dest as mostPreferredDest from table1 join table2" +
    " where table1.countOfUsers=table2.maxTravellers").show()

 /* Explanation ----->>>>>
  -> two tables are created and then join to process asked query
  -> df2 dataframe contains dest and count of users per destination
  -> from df2 table1 is created
  -> df3 dataframe is creataed using table1 and contains max of count of users
  -> from df3 table2 is created
  -> in final statement, table1 and table2 are joined where count of users in table1 matches max count of users in table2
     and based on this condition, dest of table1 is fetched
 */

  //REFER Screenshot 7 for output
}
