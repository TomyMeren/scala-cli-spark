import frameless.Job.framelessSparkDelayForJob
import frameless.TypedDataset
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col

implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
case class UpdatedSurface(city: String, surface: Int)
case class Ciudad(city: String)

val apartments = Seq(
  Apartment("Paris", 50, 300000.0, 2),
  Apartment("Paris", 100, 450000.0, 3),
  Apartment("Paris", 25, 250000.0, 1),
  Apartment("Lyon", 83, 200000.0, 2),
  Apartment("Lyon", 45, 133000.0, 1),
  Apartment("Nice", 74, 325000.0, 3)
)

val aptTypedDs = TypedDataset.create(apartments)

val updated = aptTypedDs.select(aptTypedDs('city), aptTypedDs('surface) + 2)

updated.show().run()

val updated2 = updated.as[UpdatedSurface]

updated2.show().run()

updated2.project[Ciudad].show().run()

// My model
final case class IdentityId(identityId: String) extends AnyVal
final case class Entitlements(identityId: String, granteeType: String, isTest: Boolean)

// Create DF
val data = Seq(
  Row("abc123", "identity", false, true),
  Row("def456", "identity", true, false),
  Row("ghi789", "user", false, false)
)

val schema: StructType = StructType(
  List(
    StructField("granteeId", StringType, nullable = true),
    StructField("granteeType", StringType, nullable = true),
    StructField("isTest", BooleanType, nullable = true),
    StructField("meh", BooleanType, nullable = true)
  )
)

val rdd = spark.sparkContext.parallelize(data)

val df = spark.createDataFrame(rdd, schema)

// ETL

val entitlementsDf: DataFrame = df
  .select(col("granteeId").as("identityId"), col("granteeType"), col("isTest")) // Hay que renombrar antes

val entitlementsTypedDS: TypedDataset[Entitlements] =
  TypedDataset.createUnsafe[Entitlements](entitlementsDf)

val filterTypedDS: TypedDataset[Entitlements] =
  entitlementsTypedDS
    // .filter(entitlements => entitlements.granteeType == "identity" && !entitlements.isTest) Deprecated
    .filter(entitlementsTypedDS('granteeType) === "identity" && !entitlementsTypedDS('isTest))

filterTypedDS.show().run()

val IdentityIdTypedDS: TypedDataset[IdentityId] = filterTypedDS
  // .map(row => IdentityId(row.granteeId)) deprecated
  .project[IdentityId]

IdentityIdTypedDS.show().run()
