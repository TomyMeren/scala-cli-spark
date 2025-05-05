package example

import doric._
import example.Struct.Character
import org.apache.spark.sql.Row
import org.apache.spark.sql.{functions => f}

object ExclusiveFeatures {
  def main(args: Array[String]): Unit = {
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
  }
}
