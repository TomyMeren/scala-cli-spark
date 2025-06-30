import doric.{colStruct, _}
import example.Struct._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => f}

val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

// Scala Dynamic
val df = List(1, 2, 3).toDF()

df.select(row.value[Int] + lit(1)).show(false)

// val personDfAlt = Seq(
//  RowData(1, Person("Alice", 30)),
//  RowData(2, Person("Bob", 25))
// ).toDF()

val personDf = Seq(
  (1, Person("Alice", 30)),
  (2, Person("Bob", 25))
).toDF("id", "person")

personDf.printSchema()

personDf
  .select(
    colStruct("person"),
    colStruct("person").getChild[Int]("age")
    // colStruct("person").age[Int] // No funciona. Confirmado por Alfonso
  )
  .show()

personDf
  .select(
    row.id[Int],
    row.person[Person]
    // row.person[Row].age[Int] // No funciona. Confirmado por Alfonso
  )
  .show()

// Dot Syntax
println("Dot Syntax")
val dfArrays = List(("string", Array(1, 2, 3))).toDF("str", "arr")
dfArrays.printSchema()

val complexS: Column =
  f.aggregate(f.transform(f.col("arr"), _ + 1), f.lit(0), _ + _)
dfArrays.printSchema()

dfArrays.select(complexS as "complexTransformation").show()

val complexCol: DoricColumn[Int] =
  col[Array[Int]]("arr")
    .transform(_ + 1.lit)
    .aggregate(0.lit)(_ + _)

dfArrays.select(complexCol as "complexTransformation").show()

// Implicit castings

val df0 = spark.range(1, 10).withColumn("x", f.concat(f.col("id"), f.lit("jander")))
df0.select(f.col("x")).show()

val df1 = spark.range(1, 10).toDF().withColumn("x", concat(colLong("id").cast[String], "jander".lit))
df1.select(f.col("x")).show()

val dfEq = List((1, "1"), (1, " 1"), (1, " 1 ")).toDF("int", "str")
// dfEq: org.apache.spark.sql.package.DataFrame = [int: int, str: string]

dfEq.withColumn("eq", f.col("int") === f.col("str")).show()

dfEq.withColumn("eq", colInt("int").cast[String] === colString("str")).show()

dfEq.withColumn("eq", colInt("int") === colString("str").unsafeCast[Int]).show()

// Literal conversions

val intDF = List(1, 2, 3).toDF("int")
val colS  = f.col("int") + 1
intDF.select(colS).show()

val colD = colInt("int") + 1.lit

intDF.select(colD).show()

import doric.implicitConversions.literalConversion

val colSuggarD          = colInt("int") + 1
val columConcatLiterals = concat("this", "is", "doric")

intDF.select(colSuggarD, columConcatLiterals, colInt("int") + 1).show()

/**   - row.id[Int] == col[Int]("id")
  *   - colStruct("person") == row.person[Person]
  *   - colStruct("person").getChild[Int]("age") -col[Array[Int]]("arr).transform(_ + 1.lit) * .aggregate(0.lit)(_ + _) =>
  *     promueve la nomenclatarua por puntos
  *   - concat(colLong("id").cast[String]
  *   - 1.lit, import doric.implicitConversions.literalConversion
  */
