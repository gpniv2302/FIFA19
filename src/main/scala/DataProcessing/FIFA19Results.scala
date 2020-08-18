package DataProcessing

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
object FIFA19Results {

  def TopClubWithLefFooter(spark:SparkSession,sql:SQLContext,data:DataFrame):Unit={
    import sql.implicits._
    val Leftfoot = data.filter($"Preferred Foot" === "Left").filter($"Age" < 30)
    .filter($"Position".contains("M")).filter($"Club".isNotNull)
    val LeftFootPlayerCount=Leftfoot.groupBy($"Club").agg(countDistinct($"ID") as "PlayerCount")
    val WindowSpecOrder=Window.orderBy($"PlayerCount".desc)
    val TopClubwithLeftFoot=LeftFootPlayerCount.withColumn("rank",rank().over(WindowSpecOrder))
    .filter($"rank" === 1).select($"Club")
}

  def TopClub(spark:SparkSession,sql:SQLContext,data:DataFrame): Unit ={
    data.persist(StorageLevel.MEMORY_AND_DISK)
    import sql.implicits._
    val WindowSpecClubPos=Window.partitionBy($"Club")
      .orderBy($"Overall".desc)
    //Getting MidFielders for each club
    val MidPositionsRM:List[String]=List("RM","RWM")
    val MidPositionsCM:List[String]=List("LCM","CM")
    val MidPositionsRC:List[String]=List("RCM","CM")
    val MidPositionsLM:List[String]=List("LM","LWM")
    val MidRM=data.filter($"Position".isin(MidPositionsRM:_*)).withColumn("RMrank",row_number().
    over(WindowSpecClubPos)).filter($"RMrank" === 1).drop($"RMrank")
    val MidCM=data.filter($"Position".isin(MidPositionsCM:_*)).withColumn("CMrank",row_number().
      over(WindowSpecClubPos)).filter($"CMrank" === 1).drop($"CMrank")
    val MidRC=data.filter($"Position".isin(MidPositionsRC:_*)).withColumn("RCrank",row_number().
      over(WindowSpecClubPos)).filter($"RCrank" === 1).drop($"RCrank")
    val MidLM=data.filter($"Position".isin(MidPositionsLM:_*)).withColumn("LMrank",row_number().
      over(WindowSpecClubPos)).filter($"LMrank" === 1).drop($"LMrank")
    val MidFielders=MidRM.union(MidCM).union(MidRC).union(MidLM)

    //Getting Defenders for each club
    val BackPositionsCB:List[String]=List("CB","RCB")
    val BackPositionsLC:List[String]=List("CB","LCB")
    val BackRB=data.filter($"Position"==="RB").withColumn("RBrank",row_number().
      over(WindowSpecClubPos)).filter($"RBrank" === 1).drop($"RBrank")
    val BackCB=data.filter($"Position".isin(BackPositionsCB:_*)).withColumn("CBrank",row_number().
      over(WindowSpecClubPos)).filter($"CBrank" === 1).drop($"CBrank")
    val BackLC=data.filter($"Position".isin(BackPositionsLC:_*)).withColumn("LCrank",row_number().
      over(WindowSpecClubPos)).filter($"LCrank" === 1).drop($"LCrank")
    val BackLB=data.filter($"Position"==="LB").withColumn("LBrank",row_number().
      over(WindowSpecClubPos)).filter($"LBrank" === 1).drop($"LBrank")
    val Defenders=BackRB.union(BackCB).union(BackLC).union(BackLB)

    //Getting Striker for each club
    val StrikerRF:List[String]=List("RF","CF","LF")
    val ForwardRF=data.filter($"Position".isin(StrikerRF:_*)).withColumn("RFrank",row_number().
      over(WindowSpecClubPos)).filter($"RFrank" === 1).drop($"RFrank")
    val ForwardST=data.filter($"Position"==="ST").withColumn("STrank",row_number().
      over(WindowSpecClubPos)).filter($"STrank" === 1).drop($"STrank")
    val Strikers=ForwardRF.union(ForwardST)

    //Forming Team
    val Team=Defenders.union(MidFielders).union(Strikers)
    val TenTeam=Team.groupBy($"Club").agg(count($"Position") as "PlayerCount")
      .filter($"PlayerCount" === 10).withColumnRenamed("Club","ClubTen")
    val TopClub=Team.join(TenTeam,$"Club" === $"ClubTen","inner")
      .select($"Club",$"Overall").groupBy($"Club").agg(sum($"Overall") as "OverallRating")
      .orderBy($"OverallRating".desc).first()
  }

  def ExpensiveSquad(spark:SparkSession,sql:SQLContext,data:DataFrame): Unit ={
    import sql.implicits._
    val RemoveCurrency=data.withColumn("Value_Temp",regexp_replace($"Value","[€,M,K]","").cast("Double"))
      .withColumn("Wage_Temp",regexp_replace($"Wage","[€,M,K]","").cast("Double"))
      .select($"Club",$"Value_Temp",$"Wage_Temp")
    val ClubValue=RemoveCurrency.groupBy($"Club")
      .agg(sum($"Value_Temp") as "TotalClubValue").orderBy($"TotalClubValue".desc).first()
    val TopClub=ClubValue.getString(0)
    val ClubWageValue=RemoveCurrency.groupBy($"Club")
      .agg(sum($"Wage_Temp") as "TotalClubWageValue").orderBy($"TotalClubWageValue".desc).first()
    val TopWageClub=ClubWageValue.getString(0)
    if (TopClub.equals(TopWageClub)) {
      println(s"Yes,Both Expensive Squad and Largest Wage Bill is $TopClub")
    }
    else {
      println(s"No,Expensive Squad is $TopClub and Largest Wage Bill is $TopWageClub")
    }
  }

  def PositionWage(spark:SparkSession,sql:SQLContext,data:DataFrame): Unit ={
    import sql.implicits._
    val RemoveCurrency=data
      .withColumn("Wage_Temp",regexp_replace($"Wage","[€,M,K]","").cast("Double"))
      .select($"Position",$"Wage_Temp")
    val PositionWage=RemoveCurrency.groupBy($"Position").agg(avg($"Wage_Temp") as "Avg Wage")
      .orderBy($"Avg Wage".desc).first().getString(0)
    println("Highest Position in terms of Wage:"+PositionWage)
  }
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("FIFA19").setMaster("local")
    val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sqlcontext = sparksession.sqlContext
    val FIFAData = sqlcontext.read.format("csv").option("header", true).option("inferschema", true)
      .load("/home/gokul/Maveric-THC/data.csv")
    import sqlcontext.implicits._
    val FIFARating=FIFAData.select($"Club",$"Position",$"Overall").filter($"Club".isNotNull)
      .filter($"Position".isNotNull)
    val FIFAClubValue=FIFAData.select($"Club",$"Value",$"Wage")
    val FIFAPositionWage=FIFAData.select($"Position",$"Wage")
    PositionWage(sparksession,sqlcontext,FIFAPositionWage)
    //TopClubWithLefFooter(sparksession,sqlcontext,FIFAData)
    //TopClub(sparksession,sqlcontext,FIFARating)
    //ExpensiveSquad(sparksession,sqlcontext,FIFAClubValue)
  }
}
