error id: 
file://<WORKSPACE>/SparkHelloWord.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 112
uri: file://<WORKSPACE>/SparkHelloWord.scala
text:
```scala
//> using dep org.apache.spark::spark-sql:3.5.5
//> using scala 2.13.16
//> using dep org.hablapps::doric_3-5_2.@@13:0.0.8

import org.apache.spark.sql.SparkSession

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

```


#### Short summary: 

empty definition using pc, found symbol in pc: 