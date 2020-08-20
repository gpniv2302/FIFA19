package DataProcessing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.avg

object PlayerAttributes {
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

}
