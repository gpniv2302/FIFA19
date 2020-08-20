package DataIngestion

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source.fromURL

object IngestCassandra {
  def main(args: Array[String]): Unit = {
    val reader = fromURL(getClass.getResource("Ingestion.properties")).bufferedReader()
    val properties: Properties = new Properties()
    properties.load(reader)
    val config:Map[String,String]=Map("table" -> properties.getProperty("cassandra.table"),"keyspace" -> properties.getProperty("cassandra.keyspace"))
    val sparkconf = new SparkConf().setAppName("IngestCassandra").setMaster("local")
    val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sqlcontext = sparksession.sqlContext
    import sqlcontext.implicits._
    val FIFAData = sqlcontext.read.format("csv").option("header", true).option("inferschema", true)
      .load(properties.getProperty("inputdata"))
    val LCCols=FIFAData.select(FIFAData.columns.map(x => col(x).as(x.toLowerCase)): _*)
      .write.format("org.apache.spark.sql.cassandra").options(config).save()
  }
}
