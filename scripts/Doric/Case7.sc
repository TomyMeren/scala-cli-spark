import doric._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.{Column, SparkSession}

// Dot syntax

val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

import spark.implicits._

val dfArrays = List(("string", Array(1, 2, 3))).toDF("str", "arr")
dfArrays.show()
dfArrays.printSchema()

// Spark
val complexS: Column =
  f.aggregate(f.transform(f.col("arr"), _ + 1), f.lit(0), _ + _)

dfArrays.select(complexS as "complexTransformation").show()

// Doric
val complexCol: DoricColumn[Int] =
  col[Array[Int]]("arr")
    .transform(_ + 1.lit)
    .aggregate(0.lit)(_ + _)

dfArrays.select(complexCol as "complexTransformation").show()
