package example

import doric._
import org.apache.spark.sql.{functions => f}

object Main {

  /** Before run this code, make sure to set the profile locally where workspace-url is the begining of the url of the
    * databricks workspace. For example: https://1234567890123456.7.gcp.databricks.com.
    *
    * databricks auth login --configure-cluster --host <workspace-url>
    */

  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = List(1, 2, 3)
      .toDF()
      .filter(col[Int]("value") > lit(1))
    // .select(col[Int]("value") * lit(true))

    df.show(false)

    val strDf = List("hi", "welcome", "to", "doric").toDF("str")

    strDf
      .select(f.concat(f.col("str"), f.lit("!!!")) as "newCol") // pure spark
      .select(
        concat(lit("???"), colString("newCol")) as "finalCol",
        col[String]("newCol") as "finalCol2"
      ) // pure and sweet doric
      .show()

    strDf.select(f.col("str").asDoric[String]).show()
    strDf.select(concat(f.lit("???").asDoric[String], col("str")) as "finalCol").show()

    // strDf.select((f.col("str") + f.lit(true)).asDoric[String]).show()
  }

  /**   - col[Int]("value")
    *   - colString("value")
    *   - (f.col("str").asDoric[String]
    *   - fail at compilation time .select(col[Int]("value") * lit(true))
    */
}
