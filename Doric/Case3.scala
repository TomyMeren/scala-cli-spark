import doric._
import org.apache.spark.sql.SparkSession

// Run DataFrames only when it is safe to do so (Doric Validations)
object Case3 extends App {

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  import spark.implicits._

  val df = List("1", "2", "three").toDF("value")

  // Spark
  import org.apache.spark.sql.{functions => f}
  df.select(f.col("id") + 1) // bad column name. Similar behaviour between Spark and Doric

  // Doric
  /** Similar behaviour but we now where is the error at code */
  df.select(colInt("id") + 1.lit)

  // Spark
  df.select(f.col("value") + 1).show(false) // Column exists but has bad type

  // Doric
  /** Doric is able to detect that the column exists but its type is not what we expected */
  df.select(colInt("value") + 1.lit)
  // df.select(colString("value") + 1.lit)
}
