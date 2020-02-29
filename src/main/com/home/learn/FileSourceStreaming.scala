package home.learn

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object FileSourceStreaming {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("FileSourceStreaming")
            .getOrCreate()
        val adSchema = StructType(StructField("pubId", StringType)::StructField("bidderId", StringType)
            ::StructField("imps", IntegerType)::StructField("supplyCost", IntegerType)::Nil)
        val df: sql.DataFrame = spark.readStream
            .format("csv")
            .option("header", "true")
            .schema(adSchema)
            .load("src/main/resources/filestream/")

        df.writeStream
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime(2000))
            .start()
            .awaitTermination()
    }
}
