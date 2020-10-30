package com.home.learn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object WordCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("WordCount")
            .getOrCreate()
        import spark.implicits._
        val wordCount = spark.read.text("src/resources/word.txt").as[String]
        val wc = wordCount.flatMap(_.split(" ")).map(_.replaceAll("\\pP",""))
            .groupByKey(_.toString).count().sort(desc("count(1)"))
        wc.show()
        wc.limit(10).show()
        wordCount.flatMap(_.split(" ")).createOrReplaceTempView("wordCount")
        spark.udf.register("normalize", (str: String) => str.trim()
            .toLowerCase().replaceAll("\\pP",""))
        wordCount.sqlContext.sql("select normalize(value) as word, count(*) as count " +
            "from wordCount group by word order by count desc limit 5").show()
        val df1 = spark.read.text("src/resources/word.txt").na.fill("NULL")
        spark.stop()
    }

}
