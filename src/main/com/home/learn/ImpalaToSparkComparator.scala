package com.home.learn

import com.home.learn.model.Config
import com.home.learn.utils.ConfigParser
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, round, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.apache.spark.sql.{Column, SparkSession}

object ImpalaToSparkComparator {
  //this regex_like udf is not accurate
  def replace: String => String = { s =>
    if(StringUtils.isEmpty(s) || s.equalsIgnoreCase("NULL")) {
      null
    } else {
      s
    }
  }

  val replaceUdf: UserDefinedFunction = udf(replace)

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigParser.parseConfig(args)
    val report: String = config.report
    val table: String = report.split('.')(1)
    val spark: SparkSession = SparkSession.builder()
      .appName("compare_" + report)
      .config("spark.executor.memory", "4G")
      .config("spark.driver.memory", "2G")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.port.maxRetries", "50")
      .config("spark.sql.hive.caseSensitiveInferenceMode", "NEVER_INFER")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.hadoop.fs.s3a.committer.name", "partitioned")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
        "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.debug.maxToStringFields", "100").getOrCreate()
    val df1 = spark.sql("select * from its_spark." + table)
    val df2 = spark.sql("select * from its_impala." + table)
    val formattedSchema: Array[Column] = df1.schema.fields.map {
      case StructField(name: String, _: DoubleType, _, _) => round(col(name), config.precision)
      case StructField(name: String, _: StringType, _, _) => replaceUdf.apply(col(name))
      case f => col(f.name)
    }
    val df3 = df1.select(formattedSchema: _*)
    val df4 = df2.select(formattedSchema: _*)
    val res = df3.except(df4)
    res.show(false)
  }
}
