package com.home.learn.model

import java.sql.Timestamp

case class WordInfo(time: Timestamp, word: String)
case class WordCount(time: Timestamp, word: String, count: Int)
case class WordCounter(time: String, word: String, count: BigInt)