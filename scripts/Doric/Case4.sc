import doric._

import org.apache.spark.sql.SparkSession

// Get all errors at once (Error reporting)

val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

import spark.implicits._

val dfadd = List((1, 2), (3, 4)).toDF("int1", "int2")
dfadd.show()

// Spark
import org.apache.spark.sql.{functions => f}
dfadd.withColumn("add", f.col("int_1") + f.col("int_2")) // modify

// Doric
dfadd.withColumn("add", colInt("int_1") + colInt("int_2"))

/** get all errors aggregated and the location */
