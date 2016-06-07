package org.apache.spark.streaming.datasource.models

case class InputSentences(
                           query: String,
                           offsetConditions: Option[OffsetConditions],
                           stopConditions: Option[StopConditions],
                           initialStatements: Seq[String]
                         ) {

  override def toString =
    s"[Query: $query" +
      s"\nOffsetConditions: $offsetConditions" +
      s"\nInitialStatements: ${initialStatements.mkString(" , ")}]"

  def extractLimitSentence: String =
    if (
      !query.toUpperCase.contains("LIMIT") &&
        offsetConditions.isDefined &&
        offsetConditions.get.limitRecords.isDefined
    ) s" LIMIT ${offsetConditions.get.limitRecords.get}"
    else ""
}

object InputSentences {

  def apply(
             query: String,
             offsetConditions: OffsetConditions,
             initialStatements: Seq[String]
           ): InputSentences = new InputSentences(query, Option(offsetConditions), None, initialStatements)

  def apply(
             query: String,
             stopConditions: StopConditions,
             initialStatements: Seq[String]
           ): InputSentences = new InputSentences(query, None, Option(stopConditions), initialStatements)

  def apply(
             query: String,
             offsetConditions: OffsetConditions,
             stopConditions: StopConditions,
             initialStatements: Seq[String]
           ): InputSentences = new InputSentences(query, Option(offsetConditions), Option(stopConditions), initialStatements)

  def apply(
             query: String,
             offsetConditions: OffsetConditions
           ): InputSentences = new InputSentences(query, Option(offsetConditions), None, Seq.empty[String])

  def apply(
             query: String,
             offsetConditions: OffsetConditions,
             stopConditions: StopConditions
           ): InputSentences = new InputSentences(query, Option(offsetConditions), Option(stopConditions), Seq.empty[String])

  def apply(
             query: String,
             stopConditions: StopConditions
           ): InputSentences = new InputSentences(query, None, Option(stopConditions), Seq.empty[String])

  def apply(
             query: String,
             initialStatements: Seq[String]
           ): InputSentences = new InputSentences(query, None, None, initialStatements)

  def apply(
             query: String
           ): InputSentences = new InputSentences(query, None, None, Seq.empty[String])
}