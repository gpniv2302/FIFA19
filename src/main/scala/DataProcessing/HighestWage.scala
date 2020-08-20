package DataProcessing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{avg, regexp_replace}

object HighestWage {
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
}
