package example

import doric.types._
import doric._
//import org.apache.spark.sql.{Encoders, Encoder}
//import org.apache.spark.sql.Encoders._

object Struct {
  case class Person(name: String, age: Int)
  case class Character(name: String, description: String, age: Int)

  sealed trait UserState
  object Married  extends UserState
  object Single   extends UserState
  object Divorced extends UserState

  implicit val userStateSparkType: SparkType[UserState] = SparkType[UserState]

  // implicit class UserStateOps(state: UserState) {
  //  def lit: DoricColumn[UserState] = state.lit
  // }

  /// implicit val userStateEncoder: Encoder[UserState] = Encoders.kryo[UserState]
}
