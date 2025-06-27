package example

import doric.types._
import doric._

object Struct {
  case class Person(name: String, age: Int)
  case class Character(name: String, description: String, age: Int)

  sealed trait UserState
  object Married  extends UserState
  object Single   extends UserState
  object Divorced extends UserState

  // https://github.com/hablapps/doric/blob/main/core/src/test/scala/doric/types/customTypes/User.scala
  object UserState {
    implicit val userst: SparkType[UserState] {
      type OriginalSparkType = String
    } =
      SparkType[String].customType[UserState](x => {
        x match {
          case "Married"  => Married
          case "Single"   => Single
          case "Divorced" => Divorced
        }
      })

    implicit val userlst: LiteralSparkType[UserState] {
      type OriginalSparkType = String
    } =
      LiteralSparkType[String].customType[UserState](x =>
        x match {
          case Married  => "Married"
          case Single   => "Single"
          case Divorced => "Divorced"
        }
      )
  }
}
