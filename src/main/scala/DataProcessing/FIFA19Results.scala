package DataProcessing

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.io.File
import java.io.PrintWriter
import java.util.Properties

import DataProcessing.ExpensiveSquad.ExpensiveClub
import DataProcessing.HighestWage.PositionWage
import DataProcessing.LeftFooters.TopClubWithLefFooter
import DataProcessing.PlayerAttributes.{GoalKeeperAttr, StrikerAttr}
import DataProcessing.StrongestClub.StrongestTeam

import scala.io.Source.fromURL

object FIFA19Results {
  def main(args: Array[String]): Unit = {
    val reader = fromURL(getClass.getResource("Processing.properties")).bufferedReader()
    val properties: Properties = new Properties()
    properties.load(reader)
    val OutputDir=properties.getProperty("outputdir")
    val sparkconf = new SparkConf().setAppName("FIFA19").setMaster("local")
    val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sqlcontext = sparksession.sqlContext
    import sqlcontext.implicits._
    val FIFAData = sqlcontext.read.format("csv").option("header", true).option("inferschema", true)
      .load(properties.getProperty("inputdir")).filter($"Club".isNotNull).filter($"Position".isNotNull)
      .select($"Club",$"Position",$"Overall",$"Value",$"Wage",$"Preferred Foot",$"Age",$"ID",
        $"GKDiving",$"GKHandling",$"GKKicking",$"GKPositioning",$"GKReflexes",
        $"Crossing",$"Finishing",$"HeadingAccuracy",$"ShortPassing",$"Volleys",$"Dribbling",$"Curve",$"FKAccuracy",$"LongPassing",$"BallControl"
        ,$"Acceleration",$"SprintSpeed",$"Agility",$"Reactions",$"Balance",$"ShotPower",$"Jumping",$"Stamina",$"Strength",$"LongShots",$"Aggression"
        ,$"Interceptions",$"Positioning",$"Vision",$"Penalties",$"Composure",$"Marking",$"StandingTackle",$"SlidingTackle")

    TopClubWithLefFooter(sparksession,sqlcontext,FIFAData,OutputDir)
    /*
    StrongestTeam(sparksession,sqlcontext,FIFAData,OutputDir)
    ExpensiveClub(sparksession,sqlcontext,FIFAData,OutputDir)
    PositionWage(sparksession,sqlcontext,FIFAData,OutputDir)
    GoalKeeperAttr(sparksession,sqlcontext,FIFAData,OutputDir)
    StrikerAttr(sparksession,sqlcontext,FIFAData,OutputDir)
     */
  }
}
