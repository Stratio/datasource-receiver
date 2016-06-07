package org.apache.spark.streaming.datasource.models

object OrderOperator extends Enumeration {

  type OrderRelation = Value
  val ASC, DESC = Value
}

