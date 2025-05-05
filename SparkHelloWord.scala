import org.apache.spark.sql.SparkSession
import doric._

object PlaySpark extends App {
  val spark = SparkSession
    .builder()
    .appName("PlaySpark")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(("Alice", 29), ("Bob", 33)).toDF("name", "age")
  df.show()
  spark.stop()
}
