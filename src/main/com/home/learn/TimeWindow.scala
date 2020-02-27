package com.home.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

case class Access(user: String, website: String, time: Int)

object TimeWindow {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("WordCount")
            .getOrCreate()
        import spark.implicits._
        val accesses = spark.read.format("csv")
            .option("header", "true")
            .load("src/main/resources/timewindow.csv")
            .withColumn("time", 'Time.cast(IntegerType))
            .as[Access]

        //幼稚而又愚蠢的方法
        val dummyResult = accesses.repartition($"user", $"website")
            .sortWithinPartitions($"time")
            .mapPartitions(it => {
                val list = ListBuffer[Access]()
                it.foreach(r => {
                    if(list.nonEmpty && (r.time - list.last.time <= 1)) {
                        list.remove(list.size - 1)
                    }
                    list += r
                })
                list.iterator
            })
        dummyResult.show()

        //spark dataSet方法
        val accessWindow = Window.partitionBy("user", "website").orderBy("time")
        val result = accesses
                .withColumn("lag", lag(accesses("time"), 1).over(accessWindow))
                .withColumn("diff", when(col("time").isNull, lit(0)).otherwise(col("time")) - when(col("lag").isNull, lit(0)).otherwise(col("lag")))
                .filter($"diff" > 1)
                .drop("lag", "diff")
        result.show()

        //sparkSql 方法
        accesses.createOrReplaceTempView("accesses")
        val sqlResult = accesses.sqlContext
            .sql("select user, website, time from (select user, website, time, " +
                "lag(time, 1) OVER (PARTITION BY user, website ORDER BY time) as lag " +
                "from accesses) where time - ifnull(lag, 0) > 1")
        sqlResult.show()
    }
}
