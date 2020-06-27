package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
//  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
//  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  val bounds = List(minX, maxX, minY, maxY, minZ, maxZ)
  
  pickupInfo = pickupInfo.filter(pickupInfo("x") >= minX && pickupInfo("x") <= maxX && pickupInfo("y") >= minY && pickupInfo("y") <= maxY && pickupInfo("z") >= minZ && pickupInfo("z") <= maxZ)
  pickupInfo.createOrReplaceTempView("pickupInfo")
  var group = spark.sql("select p.x, p.y, p.z, count(*) from pickupInfo as p group by p.x, p.y, p.z order by p.x, p.y, p.z")
  group.show()

  spark.udf.register("neighbors",( x: Int, y: Int, z: Int, c: Int)=>
    HotcellUtils.getNeighbors(bounds, x, y, z, c)
    )

//  var group = pickupInfo
//    .filter(pickupInfo("x") >= minX && pickupInfo("x") <= maxX && pickupInfo("y") >= minY && pickupInfo("y") <= maxY && pickupInfo("z") >= minZ && pickupInfo("z") <= maxZ)
//    .orderBy("x", "y", "z")
//    .groupBy("x", "y", "z" )
//    .count()
//  group.show()

  group.createOrReplaceTempView("groups")
  var neighbor = spark.sql("select *, neighbors(*) from groups")
  neighbor.show(false)


//  val test = group.map(x => HotcellUtils.getNeighbors(bounds, x.getAs[Int](0), x.getAs[Int](1), x.getAs[Int](2), x.getAs[Int](3) )).collect()

//  group.foreach(x <- => HotcellUtils.getNeighbors(bounds, x(0),x(1), x(2),x(3)))
//  group.collect().foreach(x => HotcellUtils.getNeighbors(bounds, x(0), x(1), x(2),x(3)))
//  for ((x, y, z, c) <- group) {
//
//  }

//  group.show()

//  group.map(x => HotcellUtils.getNeighbors(List(minX, maxX, minY, maxY, minZ, maxZ), x(0), x(1), x(2), x(3)))
//  val groupRdd = group.map(row => (row.x, row.y, row.z, row.c))
//  groupRdd.reduce()
//  groupRdd.show()
  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
