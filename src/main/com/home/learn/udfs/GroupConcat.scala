package com.home.learn.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

object GroupConcat extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType().add("x", StringType)
  def bufferSchema: StructType = new StructType().add("buff", ArrayType(StringType))
  def dataType: StringType.type = StringType
  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, ArrayBuffer.empty[String])
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0))
      buffer.update(0, buffer.getSeq[String](0) :+ input.getString(0))
  }

  def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getSeq[String](0) ++ input.getSeq[String](0))
  }

  def evaluate(buffer: Row): Any = UTF8String.fromString(
    buffer.getSeq[String](0).mkString(","))
}
