package org.apache.spark.streaming.datasource.models

object OffsetOperator extends Enumeration {

  type OrderRelation = Value

  val < = Value("<")
  val > = Value(">")
  val <= = Value("<=")
  val >= = Value(">=")

  def toOrderOperator(orderRelation: OrderRelation): OrderOperator.OrderRelation = orderRelation match {
    case `<` | `<=` => OrderOperator.ASC
    case `>` | `>=` => OrderOperator.DESC
  }

  def toProgressOperator(orderRelation: OrderRelation): OffsetOperator.OrderRelation = orderRelation match {
    case `<` | `<=` => <
    case `>` | `>=` => >
  }
}
