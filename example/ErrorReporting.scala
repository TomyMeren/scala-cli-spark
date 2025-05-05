package example

import doric._
//import org.apache.spark.sql.{functions => f}

object ErrorReporting {

  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // List(1, 2, 3).toDF().select(colInt("id") + 1.lit).show()

    val dfPair = List(("hi", 31)).toDF("str", "int")

    val col1 = colInt("str")    // existing column, wrong type
    val col2 = colString("int") // existing column, wrong type
    val col3 = colInt("unknown")

    dfPair.select(col1, col2, col3).show()

    /**   - Doric report the location of the error
      *   - Doric keep accumulating errors since Sparks adopts a fail-fast
      */
  }
}
