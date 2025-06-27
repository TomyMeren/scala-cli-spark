import doric._
import doric.types.SparkType
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.{Column, SparkSession}

// Modularize your business logic
object Case5 extends App {

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  import spark.implicits._

  val dfadd = List((1, 2), (3, 4)).toDF("int1", "int2")
  dfadd.show()

  // Spark
  val col1: Column       = f.col("int_1")
  val col2: Column       = f.col("int2")
  val addColumns: Column = col1 + col2

  dfadd.withColumn("add", addColumns)

  // Doric
  /** we will get not only the errors, but the exact location of the culprit */
  val doricCol1: DoricColumn[Int]       = colInt("int_1")
  val doricCol2: DoricColumn[Int]       = colString("int2").unsafeCast[Int]
  val addDoricColumns: DoricColumn[Int] = doricCol1 + doricCol2

  dfadd.withColumn("add", addDoricColumns)

  // extra example
  val dfPair = List(("hi", 31)).toDF("str", "int")
  // dfPair.show(false)

  val col3 = colInt("str")     // existing column, wrong type
  val col4 = colString("int")  // existing column, wrong type
  val col5 = colInt("unknown") // non-existing column

  // dfPair.select(col3, col4, col5)

  // extra example with a method
  val userDf = List(
    ("John", 25, "Madrid"),
    ("Jane", 30, "BCN"),
    ("Doe", 35, "Santander")
  ).toDF("name_user", "age_user", "city_user")
  userDf.show()

  // Spark
  def userCol(colName: String) = f.col(colName + "_user")

  userDf.select(userCol("name")).show() // OK
  userDf.select(userCol("team")).show() // Fail!

  // Doric

  // import doric.sem.Location
  def user[T: SparkType](colName: String): DoricColumn[T] = col[T](colName + "_user") //  (implicit location: Location)

  val age  = user[Int]("name")
  val team = user[String]("team")

  userDf.select(age, team).show()
}
