import doric._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.SparkSession

// Mixing Dori and Spark Columns

val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

import spark.implicits._

val strDf = List("hi", "welcome", "to", "doric").toDF("str")
strDf.show()

strDf
  .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol")      // pure spark
  .select(concat(lit("???"), colString("newCol")) as "finalCol") // pure doric
  .show()

strDf.select(f.col("str").asDoric[String]).show()

strDf.select((f.col("str") + f.lit(true)).asDoric[String]).show()
