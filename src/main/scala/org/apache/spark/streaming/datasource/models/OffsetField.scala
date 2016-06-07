/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.datasource.models

import org.apache.spark.streaming.datasource.models.OffsetOperator._

case class OffsetField(
                        name: String,
                        operator: OrderRelation = >=,
                        value: Option[Any] = None
                      ) {

  def this(name: String, operator: OrderRelation, value: Any) =
    this(name, operator, Option(value))

  def this(name: String, value: Any) =
    this(name, >=, value)

  override def toString: String =
    s"$name,$value,${operator.toString}"

  def toStringPretty: String =
    s"name: $name value: $value , operator: ${operator.toString}"

  def extractConditionSentence(sqlQuery: String): String = {
    value.fold("") { case valueExtracted =>
      val prefix = if (sqlQuery.toUpperCase.contains("WHERE")) " AND " else " WHERE "

      prefix + s" $name ${operator.toString} ${valueToSqlSentence(valueExtracted)} "
    }
  }

  def extractOrderSentence(sqlQuery: String): String =
    if (!sqlQuery.toUpperCase.contains("ORDER BY"))
      s" ORDER BY $name ${OffsetOperator.toOrderOperator(operator).toString}"
    else ""

  private def valueToSqlSentence(value: Any): String =
    if (value.isInstanceOf[String]) s"'${value.toString}'" else value.toString
}
