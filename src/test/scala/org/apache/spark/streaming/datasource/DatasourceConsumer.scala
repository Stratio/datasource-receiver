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

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.datasource.models.{InputSentences, OffsetConditions, OffsetField, StopConditions}
import org.apache.spark.streaming.datasource.models.OffsetOperator._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DatasourceConsumer {

  def main(args: Array[String]) {
    // Setup the Streaming context
    val conf = new SparkConf()
      .setAppName("datasource-receiver-example")
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val tableName = "tableName"

    val datasourceParams = Map.empty[String, String]

    val inputSentences = InputSentences(
      s"select * from $tableName",
      OffsetConditions(OffsetField("followersCount")),
      StopConditions(true, true),
      Seq("CREATE TEMPORARY TABLE tableName USING com.stratio.datasource.mongodb " +
        "OPTIONS (host 'localhost', database 'sparta', collection 'streamtrigger')"))

    val distributedStream = DatasourceUtils.createStream(ssc, inputSentences, datasourceParams)

    val totalEvents = ssc.sparkContext.accumulator(0L, "Number of events received")

    /*val sqlContext = SQLContext.getOrCreate(sc)

    val schema = new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("idInt", IntegerType, nullable = true)
    ))
    val registers = for (a <- 1 to 10000) yield Row(a.toString, a)
    val rdd = sc.parallelize(registers)

    sqlContext.createDataFrame(rdd, schema).registerTempTable(tableName)
*/
    // Start up the receiver.
    distributedStream.start()

    // Fires each time the configured window has passed.
    distributedStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val count = rdd.count()
        // Do something with this message
        println(s"\n EVENTS COUNT : \t $count")
        totalEvents += count
        //rdd.collect().foreach(event => print(s" ${event.mkString(",")} ||"))
      } else println("\n RDD is empty")
      println(s"\n TOTAL EVENTS : \t $totalEvents")
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

