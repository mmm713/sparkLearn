package main.com.home.learn

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

case class WordCount(time: Timestamp, word: String)

object SocketStreaming {
    def main(args: Array[String]): Unit = {
        //run: nc -lk 9999 in cmd before running this application
        val conf = new SparkConf()
        conf.set("spark.sql.shuffle.partitions", "5")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("FileSourceStreaming")
            .config(conf)
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
            .as[String]
            .map(l => {
                WordCount.apply(new Timestamp(System.currentTimeMillis()), l)
            })
            .as[WordCount]
            .groupBy(window($"time", "10 minutes", "5 minutes") as "time", $"word")
            .count()
            .sort("time")
            .writeStream
            .format("console")
            .outputMode(OutputMode.Complete())
            .trigger(Trigger.ProcessingTime(2000))
            .option("truncate", false)
            .start()
            .awaitTermination()
    }
}
