package home.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object FileSourceStreaming {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("FileSourceStreaming")
            .getOrCreate()
        val userSchema: StructType = new StructType()
            .add("name", StringType)
            .add("sex", StringType)
            .add("age", IntegerType)
    }
}
