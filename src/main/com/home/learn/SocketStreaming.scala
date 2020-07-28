package com.home.learn

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.home.learn.model.{WordCount, WordCounter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.collection.mutable

object SocketStreaming {
    val ord: Ordering[WordCounter] = Ordering.by[WordCounter, BigInt](_.count).reverse
    def main(args: Array[String]): Unit = {
        //in Mac, run nc -lk 9999 in cmd before running this application
        //in Windows, run nc -l -p 9999
        val conf = new SparkConf()
        conf.set("spark.sql.shuffle.partitions", "5")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SocketStreaming")
            .config(conf)
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val countStream = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
            .as[String]
            .map(l => com.home.learn.model.WordCount.apply(new Timestamp(System.currentTimeMillis()), l, 1))
            .as[WordCount]
            .withWatermark("time", "1 seconds")
            .groupBy(window($"time", "1 minutes", "10 seconds") as "time", $"word")
            .agg(sum("count") as "count")
            .repartition($"time")
            .as[WordCounter]

        val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
        format.setTimeZone(TimeZone.getTimeZone("PST"))

        countStream.mapPartitions(iter => {
                val heap = new mutable.PriorityQueue[WordCounter]()(ord)
                iter.foreach(r => {
                    if(heap.nonEmpty && heap.size >= 2) {
                        heap += r
                        heap.dequeue()
                    } else {
                        heap += r
                    }
                })
                heap.iterator
            })
            .sort("time")
            .filter(i => (System.currentTimeMillis() - format.parse(i.time.split(",")(0).substring(1)).getTime) < 30000)
            .writeStream
            .queryName("Wei Testing Word Count")
            .format("console")
            .outputMode(OutputMode.Complete())
            .trigger(Trigger.ProcessingTime(2000))
            .option("truncate", value = false)
            .start()
            .awaitTermination()
    }
}
