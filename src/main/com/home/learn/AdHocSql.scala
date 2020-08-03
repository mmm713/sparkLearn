package com.home.learn

import org.apache.spark.sql.{SaveMode, SparkSession}

object AdHocSql {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.executor.memory", "10G")
      .config("spark.debug.maxToStringFields", "100").getOrCreate()
    spark.sparkContext.getConf.getAll.foreach(println)
    val df = spark.sql("SELECT sess.visitor_id, sess.site_id, ts.test_name, ts.test_variant, email_send_7d," +
      " email_send_to_proplus_7d,  email_send_wizard_7d, email_send_non_wizard_7d, quality_reply_7d, quality_reply_wizard_7d," +
      " quality_reply_non_wizard_7d,  quality_connection_7d, " +
      " quality_connection_wizard_7d, quality_connection_non_wizard_7d" +
      " FROM ( SELECT visitor_id, site_id, sum(coalesce(email_send_7d, 0)) as email_send_7d, " +
      " sum(coalesce(email_send_to_proplus_7d, 0)) as email_send_to_proplus_7d, " +
      " sum(coalesce(email_send_wizard_7d, 0)) as email_send_wizard_7d, " +
      " sum(coalesce(email_send_non_wizard_7d, 0)) as email_send_non_wizard_7d," +
      " sum(coalesce(quality_reply_7d, 0)) as quality_reply_7d, " +
      " sum(coalesce(quality_reply_wizard_7d, 0)) as quality_reply_wizard_7d, " +
      " sum(coalesce(quality_reply_non_wizard_7d, 0)) as quality_reply_non_wizard_7d, " +
      " sum(coalesce(quality_connection_7d, 0)) as quality_connection_7d," +
      " sum(coalesce(quality_connection_wizard_7d, 0)) as quality_connection_wizard_7d, " +
      " sum(coalesce(quality_connection_non_wizard_7d, 0)) as quality_connection_non_wizard_7d " +
      " FROM abtest.session_test_pro_inquiry_7d_daily sess  " +
      " WHERE  sess.dt = \"2020-07-31\" GROUP BY visitor_id, site_id) sess" +
      " LEFT JOIN  (SELECT  visitor_id, test_name, test_variant  FROM  abtest.test_selection_visitor_daily ts " +
      " WHERE ts.dt = \"2020-07-31\") ts ON ts.visitor_id = sess.visitor_id")
    df.write.mode(SaveMode.Overwrite).parquet("s3a://houzz-impala-data/tmp/weiwang/spark/output/ab_test/")
  }
}
