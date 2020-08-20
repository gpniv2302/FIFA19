package DataProcessing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{countDistinct, rank}

object LeftFooters {

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
    Writer.write("Club which has most number of Left footed midfielders is" +" "+TopClubwithLeftFoot+"\n")
    Writer.close()
  }
}
