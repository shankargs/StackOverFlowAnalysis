package MainJob

import FileReaders.MainFileReader
import Utility.UtilFunction.read_schema
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.xml.XML

object Jobs extends App {
  val spark = SparkSession.builder.appName("StackOverFlow")
    .master("local[*]").getOrCreate()

  val cfg = ConfigFactory.load("application.conf")

  val badgeRDD: RDD[String] = MainFileReader.readFile(spark, cfg.getString("path.badges"))
  val badgeDF = spark.createDataFrame(badgeRDD.map(line => {
    val xml = XML.loadString(line)
    Row(xml.attribute("Id").mkString.toInt, xml.attribute("UserId").mkString.toInt, xml.attribute("Name").mkString,
      xml.attribute("Date").mkString, xml.attribute("Class").mkString.toInt, xml.attribute("TagBased").mkString.toBoolean)
  }), read_schema(cfg.getString("schema.badgeSchema")))

  val postHistoryRDD = MainFileReader.readFile(spark, cfg.getString("path.postHistory"))
  val postHistoryDF = spark.createDataFrame(postHistoryRDD.map(line => {
    val xml = XML.loadString(line)
    Row(xml.attribute("Id").mkString.toInt, xml.attribute("PostHistoryTypeId").mkString.toInt,
      xml.attribute("PostId").getOrElse(0).toString.toInt, xml.attribute("RevisionGUID").getOrElse("NA").toString,
      xml.attribute("CreationDate").mkString, xml.attribute("UserId").getOrElse(0).toString.toInt,
      xml.attribute("comment").getOrElse("NA").toString, xml.attribute("Text").getOrElse("NA").toString)
  }), read_schema(cfg.getString("schema.postHSchema"))
  )

  val votesRDD = MainFileReader.readFile(spark, cfg.getString("path.votes"))
  val votesDF = spark.createDataFrame(votesRDD.map(line => {
    val xml = XML.loadString(line)
    Row(xml.attribute("Id").mkString.toInt, xml.attribute("PostId").mkString.toInt, xml.attribute("VoteTypeId").mkString.toInt,
      xml.attribute("CreationDate").mkString)
  }), read_schema(cfg.getString("schema.voteSchema")))

  val tagsRDD = MainFileReader.readFile(spark, cfg.getString("path.tags"))
  val tagsDF = spark.createDataFrame(tagsRDD.map(line => {
    val xml = XML.loadString(line)
    Row(xml.attribute("Id").mkString.toInt, xml.attribute("TagName").mkString, xml.attribute("Count").mkString.toInt,
      xml.attribute("ExcerptPostId").getOrElse(0).toString.toInt, xml.attribute("WikiPostId").getOrElse(0).toString.toInt)
  }), read_schema(cfg.getString("schema.tagSchema")))

  spark.stop()
}
