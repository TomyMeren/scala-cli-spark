import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

val spark = SparkSession
  .builder()
  .appName("PlaySpark")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val df = Seq(("Alice", 29), ("Bob", 33)).toDF("name", "age")
df.select(col("name"), lit(9)).show()
spark.stop()
