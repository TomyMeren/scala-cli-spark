import doric._
import doric.sem.Location
import doric.types.SparkType
import org.apache.spark.sql.{functions => f}

val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

val userDf = List(
  ("John", 25, "Madrid"),
  ("Jane", 30, "BCN"),
  ("Doe", 35, "Santander")
).toDF("name_user", "age_user", "city_user")

userDf.printSchema()

def userCol(colName: String) = f.col(colName + "_user")

userDf.select(userCol("name")).show()

def user[T: SparkType](colName: String)(implicit location: Location): DoricColumn[T] = col[T](colName + "_user")

userDf.select(user[Int]("age")).show()

val age  = user[Int]("age")
val team = user[String]("team")

userDf.select(age, team).show()

/**   - Doric allow create methods for different types of columns def getUserField[T: SparkType](colName: String):
  *     DoricColumn[T] and the error indicate the line of the fail a not just the field name
  *   - If we added (implicit location: Location) to the method it will indicate the line where the method is used and
  *     dailing
  */
