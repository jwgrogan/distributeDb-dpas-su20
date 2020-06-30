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

  // Filter out unneeded cells and count the pickups in each cell
  pickupInfo = pickupInfo.filter(pickupInfo("x") >= minX && pickupInfo("x") <= maxX && pickupInfo("y") >= minY && pickupInfo("y") <= maxY && pickupInfo("z") >= minZ && pickupInfo("z") <= maxZ)
  pickupInfo.createOrReplaceTempView("pickupInfo")
  var group = spark.sql("select p.x, p.y, p.z, count(*) as count, count(*) * count(*) as count2 from pickupInfo as p group by p.x, p.y, p.z")
//  group.show()

  // Register neighbors function for SQL queries
  spark.udf.register("neighbors",(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int)=>
    HotcellUtils.areNeighbors(x1, y1, z1, x2, y2, z2)
    )

  // Calculate each neighbor of each cell
  // Note: cells with zero pickups are NOT included
  group.createOrReplaceTempView("groups")
  var neighbor = spark.sql("select g1.x, g1.y, g1.z, g1.count, g2.x as x2, g2.y as y2, g2.z as z2, g2.count as c2 from groups as g1, groups as g2 where neighbors(g1.x, g1.y, g1.z, g2.x, g2.y, g2.z)")
//  neighbor.show(100)
  neighbor.createOrReplaceTempView("neighbor_table")

  // TODO Calculate Gi*

  // get x_bar table
  // TODO: this is an invalid calculation; we need to update it to assume that n outside
  // of a summation is the total number of points
  //  var xbar = group.agg(sum("count")).first.get(0)
//  val x_bar = spark.sql("select x, y, z, (sum(c2) / count(c2)) as x_bar from neighbor_table group by x, y, z")
//  x_bar.show()
//  x_bar.createOrReplaceTempView("x_bar_table")

  // get S table
  // val std_dev = spark.sql("select x, y, z, sqrt((sum(power(c2, 2)) / count(c2)) - power((sum(c2) / count(c2), 2)) from neighbor_table group by x, y, z")
  // std_dev.show()

  // Calculate global xbar and S for all cells
  // Using logic provided by https://stackoverflow.com/questions/37032025/how-to-sum-the-values-of-one-column-of-a-dataframe-in-spark-scala
  val xbar = group.select("count").rdd.map(_(0).asInstanceOf[Long]).reduce(_ + _) / numCells
  val S = math.sqrt((group.select("count2").rdd.map(_(0).asInstanceOf[Long]).reduce(_ + _) / numCells) - math.pow(xbar, 2.0))

  // Add method for calculating the number of neghbors of a cell
  spark.udf.register("neighborCount", (x: Int, y: Int, z: Int) =>
    HotcellUtils.getNeighborCount(bounds, (x, y, z))
  )

  // get Gi table
  var gi_table = spark.sql("select x, y, z, sum(c2) as xi_sum, neighborCount(x, y, z) as wi_sum from neighbor_table group by x, y, z")
//  gi_table.show()

  spark.udf.register("calculateGi", (xi_sum: Int, wi_sum: Int) =>
    HotcellUtils.calculateGi(xi_sum, wi_sum, xbar, S, numCells)
  )
  // join Gi table with groups, order by Gi
//  gi_table.withColumn("gi", HotcellUtils.calculateGi(col("xi_sum"), col("wi_sum")))

  gi_table.createOrReplaceTempView("gi_table")
  var final_gi = spark.sql("select x, y, z from (select x, y, z, calculateGi(xi_sum, wi_sum) as gi from gi_table order by gi desc limit 50)") // where rownum <= 50")
//  final_gi.show(100)
  // return top 50 rows


  final_gi
}
}
