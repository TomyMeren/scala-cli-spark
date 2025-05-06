import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import example.Struct._
import org.apache.spark.sql.functions._

object UserDefinedType extends App {
  case class Point(x: Double, y: Double)

  val spark = SparkSession.builder().appName("UDT Example").master("local").getOrCreate()
  import spark.implicits._

  val df = Seq(
    (1, Point(1.0, 2.0)),
    (2, Point(3.0, 4.0))
  ).toDF("id", "point")

  df.printSchema()
  df.select(col("Point.x")).show(false)

  val df2 = Seq(
    (Married, 13),
    (Single, 55),
    (Divorced, 23)
  ).toDF("state", "Score")

  df.withColumn(
    "finalScore",
    when(col("state") === Married, col("Score") + 10)
      .when(col("state") === Single, col("Score") + 5)
      .otherwise(col("Score"))
  ).show(false)

  // no funciona

}
