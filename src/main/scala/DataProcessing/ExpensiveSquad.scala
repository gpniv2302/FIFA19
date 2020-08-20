package DataProcessing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{regexp_replace, sum}

object ExpensiveSquad {
  def ExpensiveClub(spark:SparkSession,sql:SQLContext,data:DataFrame,dir:String): Unit ={
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
}
