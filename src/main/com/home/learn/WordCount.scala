package com.home.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("WordCount")
            .getOrCreate()
        import spark.implicits._
        val wordCount = spark.read.text("src/main/resources/word.txt").as[String]
        val wc = wordCount.flatMap(_.split(" ")).groupByKey(_.toString).count().sort(desc("count(1)"))
        wc.show()
        wc.limit(10).show()
        spark.stop()
    }

}
