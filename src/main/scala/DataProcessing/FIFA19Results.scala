package DataProcessing

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.io.File
import java.io.PrintWriter

object FIFA19Results {

  def TopClubWithLefFooter(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String):Unit={
    import sql.implicits._
    val Leftfoot = data.filter($"Preferred Foot" === "Left").filter($"Age" < 30)
    .filter($"Position".contains("M")).filter($"Club".isNotNull)
    val LeftFootPlayerCount=Leftfoot.groupBy($"Club").agg(countDistinct($"ID") as "PlayerCount")
    val WindowSpecOrder=Window.orderBy($"PlayerCount".desc)
    val TopClubwithLeftFoot=LeftFootPlayerCount.withColumn("rank",rank().over(WindowSpecOrder))
    .filter($"rank" === 1).select($"Club").first().get(0)

    //Taking only one club even if the number of players same across club
    /*
    val out=TopClubwithLeftFoot.collect().map(row=>{
      sol=sol+row
    })
   */

    val Output2a = new File(dir+"2a.txt" )
    val Writer = new PrintWriter(Output2a)
    Writer.write("Club which has most number of Left footed midfielders is:" +" "+TopClubwithLeftFoot+"\n")
    Writer.close()
}

  def StrongestTeam(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
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
      .orderBy($"OverallRating".desc).first().getString(0)

    val Output2b = new File(dir+"2b.txt")
    val Writer = new PrintWriter(Output2b)
    Writer.write("Stongest team in 4-4-2 combination is" + " " + TopClub + "\n")
    Writer.close()
  }

  def ExpensiveSquad(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
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

    val Output2c = new File(dir+"2c.txt")
    val Writer = new PrintWriter(Output2c)
    if (TopClub.equals(TopWageClub)) {
      Writer.write("Yes,Both Expensive Squad and Largest Wage Bill is" + " " + TopClub + "\n")
      Writer.close()
    }
    else {
      Writer.write("No,Expensive Squad is " + TopClub + "and Largest Wage Bill club is" + " "+ TopWageClub + "\n")
      Writer.close()
    }
  }

  def PositionWage(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
    import sql.implicits._
    val RemoveCurrency=data.select($"Position",$"Wage")
      .withColumn("Wage_Temp",regexp_replace($"Wage","[€,M,K]","").cast("Double"))
      .select($"Position",$"Wage_Temp")
    val PositionWage=RemoveCurrency.groupBy($"Position").agg(avg($"Wage_Temp") as "Avg Wage")
      .orderBy($"Avg Wage".desc).first().getString(0)
    val Output2d = new File(dir+"2d.txt")
    val Writer = new PrintWriter(Output2d)
    Writer.write(PositionWage + " " + "position is highest wage" + " " + "\n")
    Writer.close()
  }

  def GoalKeeperAttr(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
    import sql.implicits._
    val MaxVal= data.filter($"Position" === "GK")
      .groupBy($"Position")
      .agg(avg($"GKDiving").cast("Double") as "max_diving", avg($"GKHandling").cast("Double") as "max_handling",
    avg($"GKKicking").cast("Double") as "max_kicking", avg($"GKPositioning").cast("Double") as "max_positioning",
    avg($"GKReflexes").cast("Double") as "max_reflexes").drop($"Position").first()
    val ColVal:List[Double]=List(MaxVal.getDouble(0),MaxVal.getDouble(1),MaxVal.getDouble(2),MaxVal.getDouble(3),MaxVal.getDouble(4))
    val TopValues:List[Double]=ColVal.sorted(Ordering.Double.reverse).take(4)
    var GKMap=Map[Double,String]()
    GKMap=GKMap+(MaxVal.getDouble(0)->"GKDiving",MaxVal.getDouble(1)->"GKHandling",MaxVal.getDouble(2)->"GKKicking",
      MaxVal.getDouble(3)->"GKPositioning",MaxVal.getDouble(4)->"GKReflexes")

    def KeyMatch(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None"
    }

   val GKAttr:String=KeyMatch(GKMap.get(TopValues(0))) + "," + KeyMatch(GKMap.get(TopValues(1))) + "," + KeyMatch(GKMap.get(TopValues(2))) + "," + KeyMatch(GKMap.get(TopValues(3)))

    val Output2e = new File(dir+"2e.txt")
    val Writer = new PrintWriter(Output2e)
    Writer.write("Top Attributes for GK are" + " " + GKAttr + "\n")
    Writer.close()
  }

  def StrikerAttr(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
    import sql.implicits._
    val AvgVal= data.filter($"Position" === "ST")
      .groupBy($"Position")
      .agg(
        avg($"Crossing").cast("Double") as "avg_crossing",avg($"Finishing").cast("Double") as "avg_finishing",avg($"HeadingAccuracy").cast("Double") as "avg_heading",avg($"ShortPassing").cast("Double") as "avg_shortpass"
        ,avg($"Volleys").cast("Double") as "avg_volleys",avg($"Dribbling").cast("Double") as "avg_dribbling",avg($"Curve").cast("Double") as "avg_curve",avg($"FKAccuracy").cast("Double") as "avg_fk"
        ,avg($"LongPassing").cast("Double") as "avg_longpassing",avg($"BallControl").cast("Double") as "avg_ballcontrol",avg($"Acceleration").cast("Double") as "avg_acceleration",avg($"SprintSpeed").cast("Double") as "avg_sprintspeed"
        ,avg($"Agility").cast("Double") as "avg_agility",avg($"Reactions").cast("Double") as "avg_reactions",avg($"Balance").cast("Double") as "avg_balance",avg($"ShotPower").cast("Double") as "avg_shotpower"
        ,avg($"Jumping").cast("Double") as "avg_jumping",avg($"Stamina").cast("Double") as "avg_stamina",avg($"Strength").cast("Double") as "avg_strength",avg($"LongShots").cast("Double") as "avg_longshots",avg($"Aggression") .cast("Double") as "avg_aggression"
        ,avg($"Interceptions").cast("Double") as "avg_interceptions",avg($"Positioning").cast("Double") as "avg_positioning",avg($"Vision").cast("Double") as "avg_vision"
        ,avg($"Penalties").cast("Double") as "avg_penalties",avg($"Composure").cast("Double") as "avg_composure",avg($"Marking").cast("Double") as "avg_marking"
        ,avg($"StandingTackle").cast("Double") as "avg_standingtackle",avg($"SlidingTackle").cast("Double") as "avg_slidingtackle"
      ).drop($"Position")
      .withColumnRenamed("avg_crossing","Crossing")
      .withColumnRenamed("avg_finishing","Finishing").withColumnRenamed("avg_heading","HeadingAccuracy")
      .withColumnRenamed("avg_shortpass","ShortPassing").withColumnRenamed("avg_volleys","Volleys")
      .withColumnRenamed("avg_dribbling","Dribbling").withColumnRenamed("avg_curve","Curve").withColumnRenamed("avg_fk","FKAccuracy")
      .withColumnRenamed("avg_longpassing","LongPassing").withColumnRenamed("avg_ballcontrol","BallControl").withColumnRenamed("avg_acceleration","Acceleration")
      .withColumnRenamed("avg_sprintspeed","SprintSpeed").withColumnRenamed("avg_agility","Agility").withColumnRenamed("avg_reactions","Reactions")
      .withColumnRenamed("avg_balance","Balance").withColumnRenamed("avg_shotpower","ShotPower").withColumnRenamed("avg_jumping","Jumping")
      .withColumnRenamed("avg_stamina","Stamina").withColumnRenamed("avg_strength","Strength").withColumnRenamed("avg_longshots","LongShots")
      .withColumnRenamed("avg_aggression","Aggression").withColumnRenamed("avg_interceptions","Interceptions")
      .withColumnRenamed("avg_positioning","Positioning").withColumnRenamed("avg_vision","Vision").withColumnRenamed("avg_penalties","Penalties")
      .withColumnRenamed("avg_composure","Composure").withColumnRenamed("avg_marking","Marking").withColumnRenamed("avg_standingtackle","StandingTackle")
      .withColumnRenamed("avg_slidingtackle","SlidingTackle")

    val Values=AvgVal.first()
    var ColVal:List[Double]=Nil
    for (i <-0 to 28){
     ColVal=Values.getDouble(i) :: ColVal
    }
    val TopValues:List[Double]=ColVal.sorted(Ordering.Double.reverse).take(5)

    val schemaList = AvgVal.schema.map(_.name).zipWithIndex
    val AttrVal=AvgVal.rdd.map(row =>
      schemaList.map(rec => (row(rec._2),rec._1)).toMap
    )
    val AttrValue=AttrVal.zipWithUniqueId().collectAsMap().keys

    def KeyMatch(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None"
    }
    var StrikerAttr:List[Any]=Nil
    val temp=AttrValue.map(line=>{
      val key=line.keys
      key.foreach(key1=>{
        if(TopValues.contains(key1)){
          StrikerAttr=KeyMatch(line.get(key1)) :: StrikerAttr
        }
      })
    })

    val StrikerAttrStr=StrikerAttr.mkString(",")
    val Output2f = new File(dir+"2f.txt")
    val Writer = new PrintWriter(Output2f)
    Writer.write("Top Attributes for Striker are" + " " + StrikerAttrStr + "\n")
    Writer.close()
  }

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("FIFA19").setMaster("local")
    val sparksession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sqlcontext = sparksession.sqlContext
    val OutputDir=args(1)
    import sqlcontext.implicits._
    val FIFAData = sqlcontext.read.format("csv").option("header", true).option("inferschema", true)
      .load(args(0)).filter($"Club".isNotNull).filter($"Position".isNotNull)
      .select($"Club",$"Position",$"Overall",$"Value",$"Wage",$"Preferred Foot",$"Age",$"ID",
        $"GKDiving",$"GKHandling",$"GKKicking",$"GKPositioning",$"GKReflexes",
        $"Crossing",$"Finishing",$"HeadingAccuracy",$"ShortPassing",$"Volleys",$"Dribbling",$"Curve",$"FKAccuracy",$"LongPassing",$"BallControl"
        ,$"Acceleration",$"SprintSpeed",$"Agility",$"Reactions",$"Balance",$"ShotPower",$"Jumping",$"Stamina",$"Strength",$"LongShots",$"Aggression"
        ,$"Interceptions",$"Positioning",$"Vision",$"Penalties",$"Composure",$"Marking",$"StandingTackle",$"SlidingTackle")

    TopClubWithLefFooter(sparksession,sqlcontext,FIFAData,OutputDir)
    StrongestTeam(sparksession,sqlcontext,FIFAData,OutputDir)
    ExpensiveSquad(sparksession,sqlcontext,FIFAData,OutputDir)
    PositionWage(sparksession,sqlcontext,FIFAData,OutputDir)
    GoalKeeperAttr(sparksession,sqlcontext,FIFAData,OutputDir)
    StrikerAttr(sparksession,sqlcontext,FIFAData,OutputDir)
  }
}
