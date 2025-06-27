package example

import doric._
import example.Struct.Character
import org.apache.spark.sql.Row
import org.apache.spark.sql.{functions => f}
import java.time.LocalDate
import org.sparkproject.jetty.server.Authentication.User

object ExclusiveFeatures extends App {
  val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Aggregations

  val customMean: DoricColumn[Double] = customAgg[Long, Row, Double](
    col[Long]("id"),          // The column to work on
    struct(lit(0L), lit(0L)), // setting the sum and the count to 0
    (x, y) =>
      struct(
        x.getChild[Long]("col1") + y,     // adding the value to the sum
        x.getChild[Long]("col2") + 1L.lit // increasing in 1 the count of elements
      ),
    (x, y) =>
      struct(
        x.getChild[Long]("col1") + y.getChild[Long]("col1"), // obtaining the total sum of all
        x.getChild[Long]("col2") + y.getChild[Long]("col2")  // obtaining the total count of all
      ),
    _ => lit[Long](3L) / lit[Long](2L) // Es necesario
    // the total sum divided by the count
  )

  spark.range(10).show()
  spark.range(10).select(customMean.as("customMean")).show()

  spark.range(10).select(customMean.as("customMean"), lit[Long](3L) / lit[Long](2L)).printSchema()

  // Custom sort for array columns & structured array columnsPermalink

  val dfArrayStruct = Seq(
    Seq(
      Character("Terminator", "T-800", 80),
      Character("Yoda", null, 900),
      Character("Gandalf", "white", 2),
      Character("Terminator", "T-1000", 1),
      Character("Gandalf", "grey", 2000)
    )
  ).toDF()

  val sparkCol = f.expr(
    "array_sort(value, (l, r) -> case " +
      // name ASC
      "when l.name < r.name then -1 " +
      "when l.name > r.name then 1 " +
      "else ( case" +
      // age DESC
      "  when l.age > r.age then -1 " +
      "  when l.age < r.age then 1 " +
      "  else 0 end " +
      ") end)"
  )

  dfArrayStruct.select(sparkCol.as("sorted")).show(false)

  val dottyArrayShort = colArray[Row]("value").sortBy(CName("name"), CNameOrd("age", Desc))

  dfArrayStruct.select(dottyArrayShort.as("sorted")).show(false)

  // Column mapping/matches

  val dfMatch = Seq("key1", "key2", "key3", "anotherKey1", "anotherKey2").toDF()
  // dfMatch: org.apache.spark.sql.package.DataFrame = [value: string]

  val mapColSpark = f
    .when(f.col("value") === "key1", "result1")
    .when(
      f.col("value") === "key2",
      "result2"
    ) // actually we could write here a different column name, so the when will not work properly
    .when(f.length(f.col("value")) > 4, "error key")
    .otherwise(null)

  dfMatch.select(mapColSpark.as("mapped")).show(false)

  val mapColDoric =
    colString("value")
      .matches[String]
      .caseW("key1".lit, "result1".lit)
      .caseW("key2".lit, "result2".lit)
      .caseW(_.length > 4.lit, "error key".lit)
      .otherwiseNull

  dfMatch.select(mapColDoric.as("mapped")).show(false)

  val transformations = Map(
    "key1" -> "result1",
    "key2" -> "result2",
    "key4" -> "result4"
  )

  val sparkFold = transformations.tail
    .foldLeft(f.when(f.col("value") === transformations.head._1, transformations.head._2)) { case (acc, (k, v)) =>
      acc.when(f.col("value") === k, v)
    }
    .otherwise(null)

  dfMatch.select(sparkFold.as("mapped")).show(false)

  val doricFold = transformations.foldLeft(colString("value").matches[String]) { case (acc, (k, v)) =>
    acc.caseW(k.lit, v.lit)
  }

  dfMatch.select(doricFold.otherwiseNull.as("mapped")).show(false)

  // Array zipWithIndex functions

  val dfArray = List(
    Array("a", "b", "c", "d"),
    Array.empty[String],
    null
  ).toDF("col1")
    .select(colArrayString("col1").zipWithIndex().as("zipWithIndex"))

  dfArray.printSchema()
  dfArray.show(false)

  // Map toArray function

  val dfMap = List(
    ("1", Map("a" -> "b", "c" -> "d")),
    ("2", Map.empty[String, String]),
    ("3", null)
  ).toDF("ix", "col")

  val dfMap2 = dfMap.select(colMapString[String]("col").toArray.as("map2Array"))

  dfMap.printSchema()

  dfMap2.printSchema()
  dfMap2.show(false)

  /**   - customAgg => Custom agregates
    *   - colArray[Row]("value").sortBy(..) => Sort Arrays
    *   - colString("value") .matches[String] .caseW(-- ) => avoid nested .when.when.otherwise
    *     - l.foldLeft(colString("value").matches[String]) { => Usefull for foldleft around a list
    *     - colArrayString("col1").z2ipWithIndex() => zipWithIndex and Array
    *     - colMapString[String]("col").toArray => from Map to Array
    */

  // Del video de Alfonso https://www.youtube.com/watch?v=mJCNIV_9plQ&t=63s
  // .caseType

  val dfDates = Seq(
    (LocalDate.parse("2023-01-01"), "2023-01-01", 1),
    (LocalDate.parse("2023-02-15"), "2023-02-15", 2),
    (LocalDate.parse("2023-03-31"), "2023-03-31", 3)
  ).toDF("colLocalDate", "colString", "colInt")

  def matchTypeT(columName: String): DoricColumn[LocalDate] =
    matchToType[LocalDate](columName)
      .caseType[LocalDate](identity)
      .caseType[String](_.toDate("yyyy-MM-dd".lit))
      // .inOtherCaseError
      .inOtherCase(LocalDate.now().lit)

  dfDates.select(matchTypeT("colLocalDate"), matchTypeT("colString"), matchTypeT("colInt")).show(false)

  // when[Int]

  val DfNum = Seq(
    (1, 0),
    (-2, 0),
    (7, 0)
  ).toDF("c0", "c1")

  val a: DoricColumn[Int] =
    when[Int]
      .caseW(col[Int]("c0") > 4.lit, col[Int]("c1") + 5.lit)
      .caseW(col[Int]("c0") < 0.lit, col[Int]("c1") + 4.lit)
      .otherwise(0.lit)
  // otherwiseNulll

  DfNum.withColumn("newColumn", a).show()

  // Hig order

  val dataNumAndStringDf = Seq(
    ("A", 1),
    ("A", 2),
    ("B", 2),
    ("A", 2),
    ("B", 4)
  ).toDF("grupo", "valor")

  val aggFunction: DoricColumn[Array[Int]] = collectList(col[Int]("valor"))
  val filtered: DoricColumn[Array[Int]]    = aggFunction.filter(_ < 3.lit)
  val superAgg: DoricColumn[Int]           = filtered.aggregate(0.lit)(_ + _)

  val superAgg2: DoricColumn[Int] = collectList(col[Int]("valor"))
    .filter(_ < 3.lit)
    .aggregate(0.lit)(_ + _)

  dataNumAndStringDf.select(superAgg as "agg1", superAgg2 as "agg2").show(false)

  // Interpolators

  val test = ds"I'm ${col[Int]("valor").cast[String]} old"

  dataNumAndStringDf.select(test as "interpolator").show(false)

  // custom types

  import example.Struct._
  import doric.implicitConversions.literalConversion

  val dfState = Seq(
    ("Married", 1),
    ("Single", 2),
    ("Divorced", 3)
  ).toDF("state", "score")

  val scoreByState: IntegerColumn = when[Int]
    .caseW(col[UserState]("state") === Single, col[Int]("score") * 2.lit)
    .caseW(col[UserState]("state") === Married, col[Int]("score") * 10.lit)
    .otherwise(col[Int]("score"))

  dfState.select(scoreByState.as("scoreByState")).show(false)

  // No funciona

}
