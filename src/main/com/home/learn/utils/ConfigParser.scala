package com.home.learn.utils

import com.home.learn.model.Config
import org.apache.commons.lang3.StringUtils
import scopt.OParser

object ConfigParser {
  def parseConfig(args: Array[String]): Config = {
      val builder = OParser.builder[Config]
      val parser = {
      import builder._
      OParser.sequence(programName("config parser"),
        head("scopt", "4.x"),
        opt[String]('r', "report")
          .action((x, c) => c.copy(report = x))
          .validate(x => if (StringUtils.isNotEmpty(x)) success else failure("must enter valid report name"))
          .text("report is the report name to run"),
        opt[String]('q', "query")
          .action((x, c) => c.copy(query = x)),
        opt[String]('o', "output-path")
          .action((x, c) => c.copy(outputPath = x)),
        opt[String]('i', "impala-type")
          .action((x, c) => c.copy(impalaType = x)),
        opt[Int]('p', "precision")
          .action((x, c) => c.copy(precision = x)),
        opt[Unit]('m', "schema-merge")
          .action((_, c) => c.copy(schemaMerge = true)),
        opt[Int]('l', "load-set")
          .action((x, c) => c.copy(loadSet = x)),
        help("help").text("this is a adhoc sql helper to run existing reports")
      )
    }
    val conf: Config = OParser.parse(parser, args, Config()).get
    conf.resourceConf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.port.maxRetries", "50")
      .set("spark.sql.hive.caseSensitiveInferenceMode", "NEVER_INFER")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "partitioned")
      .set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .set("spark.debug.maxToStringFields", "100")
    conf.loadSet match {
      case 1 => conf.resourceConf.set("spark.executor.memory", "4G")
        conf.resourceConf.set("spark.driver.memory", "1G")
      case 2 => conf.resourceConf.set("spark.executor.memory", "8G")
        conf.resourceConf.set("spark.driver.memory", "3G")
      case 3 => conf.resourceConf.set("spark.executor.memory", "16G")
        conf.resourceConf.set("spark.driver.memory", "6G")
        conf.resourceConf.set("spark.dynamicAllocation.minExecutors", "10")
        conf.resourceConf.set("spark.dynamicAllocation.executorIdleTimeout", "100s")
      case 4 => conf.resourceConf.set("spark.executor.memory", "32G")
        conf.resourceConf.set("spark.driver.memory", "10G")
        conf.resourceConf.set("spark.memory.offHeap.size", "16G")
        conf.resourceConf.set("spark.memory.offHeap.enabled", "true")
        conf.resourceConf.set("spark.dynamicAllocation.minExecutors", "20")
        conf.resourceConf.set("spark.dynamicAllocation.executorIdleTimeout", "300s")
      case 5 => conf.resourceConf.set("spark.executor.memory", "64G")
        conf.resourceConf.set("spark.driver.memory", "16G")
        conf.resourceConf.set("spark.memory.offHeap.size", "32G")
        conf.resourceConf.set("spark.memory.offHeap.enabled", "true")
        conf.resourceConf.set("spark.sql.autoBroadcastJoinThreshold", "2147483648")
        conf.resourceConf.set("spark.dynamicAllocation.executorIdleTimeout", "600s")
        conf.resourceConf.set("spark.dynamicAllocation.minExecutors", "60")
      case _ => print("no preset resource set load")
    }
    conf
  }
}
