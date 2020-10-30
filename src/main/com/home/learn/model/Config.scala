package com.home.learn.model

import com.home.learn.model.ImpalaType.{ImpalaType, impalaAlpha}
import org.apache.spark.SparkConf

case class Config(
                 report: String = "",
                 query: String = "",
                 outputPath: String = "",
                 schemaMerge: Boolean = false,
                 precision: Int = 10,
                 resourceConf: SparkConf = new SparkConf(),
                 loadSet: Int = 0,
                 impalaType: ImpalaType = impalaAlpha
)

object ImpalaType extends Enumeration {
  type ImpalaType = String
  val impalaMain = "impala-main"
  val impalaAlpha = "impala-alpha"
}