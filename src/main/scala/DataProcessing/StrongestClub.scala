package DataProcessing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number, sum}
import org.apache.spark.storage.StorageLevel

object StrongestClub {
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
}
