package org.apache.spark.streaming.datasource.models

case class StopConditions(stopWhenEmpty: Boolean = false, finishContextWhenEmpty: Boolean = false)
