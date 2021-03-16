package FileReaders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MainFileReader {
  def readFile(spark: SparkSession, filePath: String): RDD[String] = {
    val dataRDD = spark.read.textFile(filePath).rdd
    dataRDD.filter(_.trim.startsWith("<row "))
  }
}
