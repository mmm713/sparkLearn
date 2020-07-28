package com.home.learn

import java.sql.Timestamp

import com.home.learn.app.WordCountTopK
import com.home.learn.model.{WordCount, WordInfo}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStreamingTopK {
    def main(args: Array[String]): Unit = {
        //in Mac, run nc -lk 9999 in cmd before running this application
        //in Windows, run nc -l -p 9999
        val conf = new SparkConf().setMaster("local[*]").setAppName("SocketStreamingTopK")
        conf.set("spark.sql.shuffle.partitions", "5")
        val scc: StreamingContext = new StreamingContext(conf, Seconds(5))
        scc.checkpoint("src/main/resources/temp/checkpoint/")
        scc.sparkContext.setLogLevel("ERROR")

        val wordStream: DStream[WordInfo] = scc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
            .map(l => WordInfo.apply(new Timestamp(System.currentTimeMillis()), l))

        WordCountTopK.statWordCount(wordStream, 3)
        scc.start()
        scc.awaitTermination()
    }
}
