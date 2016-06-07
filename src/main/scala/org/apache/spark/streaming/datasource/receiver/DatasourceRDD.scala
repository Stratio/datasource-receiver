package org.apache.spark.streaming.datasource.receiver

import org.apache.spark.partial.{BoundedDouble, CountEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.datasource.config.ParametersUtils
import org.apache.spark.streaming.datasource.models.{InputSentences, OffsetOperator}
import org.apache.spark.{Logging, Partition, TaskContext}

private[datasource]
class DatasourceRDD(
                     @transient sqlContext: SQLContext,
                     inputSentences: InputSentences,
                     datasourceParams: Map[String, String]
                   ) extends RDD[Row](sqlContext.sparkContext, Nil) with Logging with ParametersUtils {

  private var totalCalculated: Option[Long] = None

  val querySentence = inputSentences.offsetConditions.fold(inputSentences.query) { case offset =>
    inputSentences.query +
      offset.fromOffset.extractConditionSentence(inputSentences.query) +
      offset.fromOffset.extractOrderSentence(inputSentences.query) +
      inputSentences.extractLimitSentence
  }

  val dataFrame = sqlContext.sql(querySentence)

  def progressInputSentences: InputSentences = {
    if (!dataFrame.rdd.isEmpty()) {
      inputSentences.offsetConditions.fold(inputSentences) { case offset =>
        val offsetValue = dataFrame.rdd.first().get(dataFrame.schema.fieldIndex(offset.fromOffset.name))

        inputSentences.copy(offsetConditions = Option(offset.copy(fromOffset = offset.fromOffset.copy(
          value = Option(offsetValue),
          operator = OffsetOperator.toProgressOperator(offset.fromOffset.operator)))))
      }
    } else inputSentences
  }

  /**
   * Return the number of elements in the RDD. Optimized when is called the second place
   */
  override def count(): Long = {
    totalCalculated.getOrElse {
      totalCalculated = Option(inputSentences.offsetConditions.fold(dataFrame.count()) { case conditions =>
        conditions.limitRecords.getOrElse(dataFrame.count())
      })
      totalCalculated.get
    }
  }

  /**
   * Return the number of elements in the RDD approximately. Optimized when count are called before
   */
  override def countApprox(
                            timeout: Long,
                            confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    if (totalCalculated.isDefined) {
      val c = count()
      new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
    } else {
      withScope {
        val countElements: (TaskContext, Iterator[Row]) => Long = { (ctx, iter) =>
          var result = 0L
          while (iter.hasNext) {
            result += 1L
            iter.next()
          }
          result
        }
        val evaluator = new CountEvaluator(partitions.length, confidence)
        sqlContext.sparkContext.runApproximateJob(this, countElements, evaluator, timeout)
      }
    }
  }

  /**
   * Return if the RDD is empty. Optimized when count are called before
   */
  override def isEmpty(): Boolean = {
    totalCalculated.fold {
      withScope {
        partitions.length == 0 || take(1).length == 0
      }
    } { total => total == 0L }
  }

  override def getPartitions: Array[Partition] = dataFrame.rdd.partitions

  override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = dataFrame.rdd.compute(thePart, context)

  override def getPreferredLocations(thePart: Partition): Seq[String] = dataFrame.rdd.preferredLocations(thePart)
}