import org.apache.spark.sql.SparkSession
import org.graphframes._

// scala-cli run project.scala scripts/GraphFrame/GraphFrameQuickStart.scala
// scala-cli run . --main-class GraphFrameQuickStart

object GraphFrameQuickStart extends App {
  val spark = SparkSession
    .builder()
    .appName("PlaySpark")
    .master("local[*]")
    .getOrCreate()

// Create a Vertex DataFrame with unique ID column "id"
  val v = spark
    .createDataFrame(
      List(
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30)
      )
    )
    .toDF("id", "name", "age")

// Create an Edge DataFrame with "src" and "dst" columns
  val e = spark
    .createDataFrame(
      List(
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow")
      )
    )
    .toDF("src", "dst", "relationship")
// Create a GraphFrame
  val g = org.graphframes.GraphFrame(v, e)

// Query: Get in-degree of each vertex.
  g.inDegrees // .show()

// Query: Count the number of "follow" connections in the graph.
  // println(g.edges.filter("relationship = 'follow'").count())

// Run PageRank algorithm, and show results.
  val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
  results.vertices.select("id", "pagerank").show()
}
