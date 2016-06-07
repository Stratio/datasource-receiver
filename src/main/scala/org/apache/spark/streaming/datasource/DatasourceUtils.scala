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

package org.apache.spark.streaming.datasource

import java.util.{Map => JMap}

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.datasource.models.InputSentences
import org.apache.spark.streaming.datasource.receiver.DatasourceDStream
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.JavaConverters._

object DatasourceUtils {

  /**
   *
   * Create an input stream that receives messages from a Datasource queue in distributed mode, each executor can
   * consume messages from a different cluster or queue-exchange. Is possible to parallelize the consumer in one
   * executor creating more channels, this is transparent to the user.
   * The result DStream is mapped to the type R with the function messageHandler.
   *
   * @param ssc              StreamingContext object
   * @param inputSentences   Object that can contains the connections to the queues, it can be more than one and each
   *                         tuple of queue, exchange, routing key and hosts can be one Datasource independent
   * @param datasourceParams Datasource params with queue options, spark options and consumer options
   * @return The new DStream with the messages consumed and parsed to the R type
   */
  def createStream(
                    ssc: StreamingContext,
                    inputSentences: InputSentences,
                    datasourceParams: Map[String, String]
                  ): InputDStream[Row] = {

    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)

    new DatasourceDStream(ssc, inputSentences, datasourceParams, sqlContext)
  }

  def createStream[C <: SQLContext](
                                     ssc: StreamingContext,
                                     inputSentences: InputSentences,
                                     datasourceParams: Map[String, String],
                                     sqlContext: C
                                   ): InputDStream[Row] = {

    new DatasourceDStream(ssc, inputSentences, datasourceParams, sqlContext)
  }

  /**
   *
   * Create an input stream that receives messages from a Datasource queue in distributed mode, each executor can
   * consume messages from a different cluster or queue-exchange. Is possible to parallelize the consumer in one
   * executor creating more channels, this is transparent to the user.
   * The result DStream is mapped to the type R with the function messageHandler.
   *
   * @param javaStreamingContext JavaStreamingContext object
   * @param inputSentences       Object that can contains the connections to the queues, it can be more than one and each
   *                             tuple of queue, exchange, routing key and hosts can be one Datasource independent
   * @param datasourceParams     Datasource params with queue options, spark options and consumer options
   * @return The new DStream with the messages consumed and parsed to the R type
   */
  /*def createJavaStream(
                        javaStreamingContext: JavaStreamingContext,
                        inputSentences: InputSentences,
                        datasourceParams: JMap[String, String]
                      ): InputDStream[Row] = {

    new DatasourceDStream(javaStreamingContext.ssc, inputSentences, datasourceParams.asScala.toMap)
  }*/
}
