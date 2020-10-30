package com.home.learn

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit}

object PlayGround {

  def main(args: Array[String]): Unit = {
    println("hello world")
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("PlayGround").getOrCreate()
    val df = spark.read.option("header", "true").csv("src/resources/concat.csv")
    df.show()
    df.coalesce(1)
    val res = df.select(concat(col("value0"), lit(","), col("value1"), lit(","), col("value2"), lit(","), col("value3")).as("concat"))
    res.show(false)
    res.coalesce(1).write.option("quoteAll", "true").mode(SaveMode.Overwrite).csv("src/resources/concat_res")
    df.coalesce(1).write.option("quoteAll", "true").mode(SaveMode.Overwrite).csv("src/resources/no_concat_res")
  }
}

