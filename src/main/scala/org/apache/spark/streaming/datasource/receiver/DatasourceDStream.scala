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

package org.apache.spark.streaming.datasource.receiver

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.datasource.config.ConfigParameters._
import org.apache.spark.streaming.datasource.config.ParametersUtils
import org.apache.spark.streaming.datasource.models.InputSentences
import org.apache.spark.streaming.datasource.receiver.DatasourceDStream._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

private[streaming]
class DatasourceDStream[C <: SQLContext](
                                          @transient val _ssc: StreamingContext,
                                          val inputSentences: InputSentences,
                                          val datasourceParams: Map[String, String],
                                          val sqlContext: C
                                        ) extends InputDStream[Row](_ssc) with Logging with ParametersUtils {

  private[streaming] override def name: String = s"Datasource stream [$id]"

  storageLevel = calculateStorageLevel()

  inputSentences.initialStatements.foreach(statement => sqlContext.sql(statement))

  /**
   * Min storage level is MEMORY_ONLY, because compute function for one rdd is called more than one place
   */
  private[streaming] def calculateStorageLevel(): StorageLevel = {
    val levelFromParams = getStorageLevel(datasourceParams)
    if (levelFromParams == StorageLevel.NONE) {
      log.warn("NONE is not a valid storage level for this datasource receiver, setting it in MEMORY_ONLY")
      StorageLevel.MEMORY_ONLY
    } else levelFromParams
  }

  /**
   * Remember duration for the rdd created by this InputDStream,
   * by default DefaultMinRememberDuration = 60s * slideWindow
   */
  private val userRememberDuration = getRememberDuration(datasourceParams)
  private val stopGracefully = getStopGracefully(datasourceParams)
  private val stopSparkContext = getStopSparkContext(datasourceParams)


  userRememberDuration match {
    case Some(duration) =>
      remember(Seconds(duration))
    case None =>
      val minRememberDuration = Seconds(ssc.conf.getTimeAsSeconds(
        ssc.conf.get("spark.streaming.minRememberDuration", DefaultMinRememberDuration), DefaultMinRememberDuration))
      val numBatchesToRemember = math.ceil(minRememberDuration.milliseconds / slideDuration.milliseconds).toInt

      remember(slideDuration * numBatchesToRemember)
  }

  /**
   * Calculate the max number of records that the receiver must receive and process in one batch when the
   * blackPressure is enable
   */
  private[streaming] def maxRecords(): Option[(Int, Long)] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    estimatedRateLimit.flatMap(estimatedRateLimit => {
      if (estimatedRateLimit > 0) {
        val recordsRateController = ((slideDuration.milliseconds.toDouble / 1000) * estimatedRateLimit).toLong

        Option((estimatedRateLimit, getMaxRecordsWithRate(recordsRateController)))
      } else None
    })
  }

  /**
   *
   * @return max number of records that the input RDD must receive in the next window
   */
  private[streaming] def getMaxRecordsWithRate(recordsRateController: Long): Long = {
    inputSentences.offsetConditions.fold(recordsRateController) { offsetConditions =>
      offsetConditions.limitRecords.fold(recordsRateController) { maxRecordsPartition =>
        Math.min(recordsRateController, maxRecordsPartition)
      }
    }
  }

  override def compute(validTime: Time): Option[DatasourceRDD] = {

    if (!computingStopped) {
      // Report the record number and metadata of this batch interval to InputInfoTracker and calculate the maxRecords
      val maxRecordsCalculation = maxRecords().map { case (estimated, newLimitRecords) =>
        val description =
          s"LimitRecords : ${
            inputSentences.offsetConditions.fold("") {
              _.limitRecords.fold("") {
                _.toString
              }
            }
          }\t:" + s" Estimated: $estimated\t NewLimitRecords: $newLimitRecords"

        (StreamInputInfo.METADATA_KEY_DESCRIPTION -> description, newLimitRecords)
      }
      val metadata = Map("InputSentences" -> inputSentences) ++
        maxRecordsCalculation.fold(Map.empty[String, Any]) { case (description, _) => Map(description) }
      val inputSentencesCalculated = progressInputSentences.getOrElse(inputSentences)
      val inputSentencesLimited = maxRecordsCalculation.fold(inputSentencesCalculated) { case (_, maxMessages) =>
        inputSentencesCalculated.copy(offsetConditions = inputSentencesCalculated.offsetConditions.map(conditions =>
          conditions.copy(limitRecords = Option(maxMessages))))
      }
      val datasourceRDD = new DatasourceRDD(sqlContext, inputSentencesLimited, datasourceParams)
      val rddSize = datasourceRDD.count()

      //publish data in Spark UI
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, StreamInputInfo(id, rddSize, metadata))

      progressInputSentences = Option(datasourceRDD.progressInputSentences)

      if (rddSize == 0 && inputSentences.stopConditions.fold(false) {_.stopWhenEmpty})
        computingStopped = true

      Option(datasourceRDD)
    } else {
      if (inputSentences.stopConditions.isDefined && inputSentences.stopConditions.get.finishContextWhenEmpty)
        _ssc.stop(stopSparkContext, stopGracefully)
      None
    }
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf))
      Option(new DatasourceRateController(id, RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    else None
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DatasourceRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {

    override def publish(rate: Long): Unit = {
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
    }
  }

}

private[streaming] object DatasourceDStream {

  /**
   * Control the new calculated offsets
   */
  var progressInputSentences: Option[InputSentences] = None

  /**
   * Computing stopped
   */
  var computingStopped: Boolean = false
}

