error id: file://<WORKSPACE>/doriccases/Case1.scala:0
file://<WORKSPACE>/doriccases/Case1.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:

offset: 0
uri: file://<WORKSPACE>/doriccases/Case1.scala
text:
```scala
@@

import doric._
import org.apache.spark.sql.SparkSession

// Get rid of malformed column expressions at compile time
object Case1 extends App {

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  import spark.implicits._

  val df = List(1, 2, 3).toDF()
  df.show()

  // Spark
  import org.apache.spark.sql.{functions => f}
  df.select($"value" * f.lit(true))

  // Doric
  /** Error are reported at compile-time */
  // df.select(col[Int]("value") * lit(true))

  df.withColumn("other", colInt("value") * lit(1))
  df.filter(colInt("value") > lit(3))
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 