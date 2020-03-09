package main.com.home.learn.app

import java.sql.Timestamp

import main.com.home.learn.model.WordInfo
import org.apache.spark.streaming.dstream.DStream

object WordCountTopK {
    def statWordCount(wordStream: DStream[WordInfo], n: Int) = {
        val value: DStream[(Timestamp, (String, Int))] = wordStream
            .map(word => ((word.time, word.word), 1))
            .updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
                Some(seq.sum + option.getOrElse(0))
            }).map {
            case ((time, word), count) => (time, (word, count))
        }
        //分组 排序
        value.groupByKey
            .map{
                case (time, wordIterable) =>
                    (time, wordIterable.toList.sortBy(_._2).take(n))
            }
    }
}
