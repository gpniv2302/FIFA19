package DataIngestion

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.io.Source.fromURL

object IngestPostgres {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("IngestPostgres").setMaster("local")
    val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sqlcontext = sparksession.sqlContext
    import sqlcontext.implicits._
    val FIFAData = sqlcontext.read.format("csv").option("header", true).option("inferschema", true)
      .load(args(0)).filter($"Club".isNotNull).filter($"Position".isNotNull)
      .select($"Overall",$"Position",$"Nationality",$"Name",$"Club",$"Wage",$"Value",$"Joined",$"Age")
    val FormatCols=FIFAData.withColumnRenamed("Joined","Date Joined")
      .withColumnRenamed("Wage","WageCurrency").withColumnRenamed("Value","ValueCurrency")
      .withColumn("Joined",to_date($"Date Joined","MMM dd, yyyy"))
        .withColumn("Wage",regexp_replace($"WageCurrency","[€,M,K]","").cast("Decimal") * 1000)
      .withColumn("Value",regexp_replace($"ValueCurrency","[€,M,K]","").cast("Decimal") * 1000000)
      .select($"Overall".cast("Int"),$"Position",$"Nationality",$"Name",$"Club",$"Wage",$"Value",$"Joined",$"Age")

      val reader = fromURL(this.asInstanceOf[Any].getClass.getResource("Ingestion.properties")).bufferedReader()
      val properties: Properties = new Properties()
      properties.load(reader)
      val jdbcurl = properties.getProperty("postgres.connection.url")
      val table=properties.getProperty("postgres.tablename")
      val connectionProperties = new Properties()
      connectionProperties.setProperty("Driver", "org.postgresql.Driver")
      connectionProperties.setProperty("user",properties.getProperty("postgres.username"))
      connectionProperties.setProperty("password",properties.getProperty("postgres.password"))
      FormatCols.write.jdbc(jdbcurl,table,connectionProperties)
  }
}
