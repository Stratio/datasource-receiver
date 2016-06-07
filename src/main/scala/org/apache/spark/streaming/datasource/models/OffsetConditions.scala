package org.apache.spark.streaming.datasource.models

case class OffsetConditions(fromOffset: OffsetField,
                            limitRecords: Option[Long]) {

  override def toString =
    s"[FromOffsets: $fromOffset" +
      s"LimitRecords: ${limitRecords.getOrElse("")}]"
}

object OffsetConditions {

  def apply(fromOffset: OffsetField,
            limitRecords: Long): OffsetConditions = new OffsetConditions(fromOffset, Option(limitRecords))

  def apply(fromOffset: OffsetField): OffsetConditions = new OffsetConditions(fromOffset, None)
}
