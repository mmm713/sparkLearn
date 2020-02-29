package com.home.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

case class Record(user: String, word: String, count: Int)

object TopKCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("WordCount")
            .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")
        val records = spark.read.format("csv")
            .option("header", "true")
            .load("src/main/resources/topk.csv")
            .withColumn("count", 'Count.cast(IntegerType))
            .as[Record]
        //spark dataSet方法
        val recSum = records.groupBy($"user", $"word").agg(sum("count") as "count")
        val orderWindow = Window.partitionBy("user").orderBy(desc("count"))
        val topKCount = recSum.withColumn("rank", rank over orderWindow).filter($"rank" < 3)
        topKCount.drop("rank").show()
        //sparkSql 方法
        records.createOrReplaceTempView("records")
        val result = records.sqlContext
            .sql("select user, word, count from (select user, word, sum(count) as count, " +
                "dense_rank() OVER (PARTITION BY user ORDER BY sum(count) DESC) as rank " +
                "from records " +
                "group by user, word) where rank < 3")
        result.show()
        spark.stop()
    }
}
