import doric._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.SparkSession

// Avoid implicit type castings
object Case2 extends App {

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  import spark.implicits._

  val df = spark.range(1, 10).toDF
  df.show(false)

  // Spark
  df.withColumn("x", f.concat(f.col("id"), f.lit("jander"))).show()

  // Doric
  /** Avoid implicit type conversion */
  // df.withColumn("x", concat(colLong("id"), "jander".lit)).show() // .cast[String]

  /** -------------------------------- Another example -------------------------------- */
  val dfEq = List((1, "1"), (1, " 1"), (1, " 1 ")).toDF("int", "str")
  dfEq.show()

  // Spark
  dfEq.withColumn("eq", f.col("int") === f.col("str")).show

  // Doric
  /** More control over type behaviour */

  // Option 1, no castings: compile error
  // dfEq.withColumn("eq", colInt("int") === colString("str")).show

  // Option 2, casting from int to string
  dfEq.withColumn("eq", colInt("int").cast[String] === colString("str")).show

  // Option 3, casting from string to int, not safe!
  dfEq.withColumn("eq", colInt("int") === colString("str").unsafeCast[Int]).show
}
