package example

import doric._
import org.apache.spark.sql.{functions => f}

object Validations extends App {
  val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // List(1, 2, 3).toDF().select(f.col("id") + 1).show()
  // List(1, 2, 3).toDF().select(colInt("id") + 1.lit).show()

  val df = List("1", "2", "three").toDF()

  df.select(f.col("value") + 1).show()

  df.select(colInt("value") + 1.lit).show()

  /**   - Doric doesn't detect columns that doens't exists as Spark
    *   - But Doric detect the columns type
    */
}
