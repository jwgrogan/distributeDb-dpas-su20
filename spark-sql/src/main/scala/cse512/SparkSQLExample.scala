package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.math.{sqrt, pow}

object SparkSQLExample {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CSE512-Phase2")
      .config("spark.some.config.option", "some-value").master("local[*]")
      .getOrCreate()

    paramsParser(spark, args)

    spark.stop()
  }

  private def paramsParser(spark: SparkSession, args: Array[String]): Unit =
  {
    var paramOffset = 1
    var currentQueryParams = ""
    var currentQueryName = ""
    var currentQueryIdx = -1

    while (paramOffset <= args.length)
    {
        if (paramOffset == args.length || args(paramOffset).toLowerCase.contains("query"))
        {
          // Turn in the previous query
          if (currentQueryIdx!= -1) queryLoader(spark, currentQueryName, currentQueryParams, args(0)+currentQueryIdx)

          // Start a new query call
          if (paramOffset == args.length) return

          currentQueryName = args(paramOffset)
          currentQueryParams = ""
          currentQueryIdx = currentQueryIdx+1
        }
        else
        {
          // Keep appending query parameters
          currentQueryParams = currentQueryParams + args(paramOffset) +" "
        }

      paramOffset = paramOffset+1
    }
  }

  private def queryLoader(spark: SparkSession, queryName:String, queryParams:String, outputPath: String): Unit =
  {
    var queryResult:Long = -1
    val queryParam = queryParams.split(" ")
    if (queryName.equalsIgnoreCase("RangeQuery"))
    {
      if(queryParam.length!=2) throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 2 parameters but you entered "+queryParam.length)
      queryResult = SpatialQuery.runRangeQuery(spark, queryParam(0), queryParam(1))
    }
    else if (queryName.equalsIgnoreCase("RangeJoinQuery"))
    {
      if(queryParam.length!=2) throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 2 parameters but you entered "+queryParam.length)
      queryResult = SpatialQuery.runRangeJoinQuery(spark, queryParam(0), queryParam(1))
    }
    else if (queryName.equalsIgnoreCase("DistanceQuery"))
    {
      if(queryParam.length!=3) throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 3 parameters but you entered "+queryParam.length)
      queryResult = SpatialQuery.runDistanceQuery(spark, queryParam(0), queryParam(1), queryParam(2))
    }
    else if (queryName.equalsIgnoreCase("DistanceJoinQuery"))
    {
      if(queryParam.length!=3) throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 3 parameters but you entered "+queryParam.length)
      queryResult = SpatialQuery.runDistanceJoinQuery(spark, queryParam(0), queryParam(1), queryParam(2))
    }
    else
    {
        throw new NoSuchElementException("[CSE512] The given query name "+queryName+" is wrong. Please check your input.")
    }

    import spark.implicits._
    val resultDf = Seq(queryName, queryResult.toString).toDF()
    resultDf.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  val ST_Contains = (queryRectangle:String, pointString:String) =>
  {
    // get point longitude and latitude
    val point = pointString.split(",")
    val pointLon = point(0).toDouble
    val pointLat = point(1).toDouble

    // get rectangle points
    val rectPoints = queryRectangle.split(",")
    val upperLeft = rectPoints.slice(0, 2)
    var upperLeftLon = upperLeft(0).toDouble
    var upperLeftLat = upperLeft(1).toDouble

    val lowerRight = rectPoints.slice(2, 4)
    var lowerRightLon = lowerRight(0).toDouble
    var lowerRightLat = lowerRight(1).toDouble

    // Make sure upper values are larger than lower values
    if (upperLeftLon < lowerRightLon)
    {
      upperLeftLon = lowerRightLon
      lowerRightLon = upperLeft(0).toDouble
    }
    if (upperLeftLat < lowerRightLat)
    {
      upperLeftLat = lowerRightLat
      lowerRightLat = upperLeft(1).toDouble
    }

    // check longitude
    if (upperLeftLon >= pointLon && lowerRightLon <= pointLon)
    {
      // check latitude
      if (upperLeftLat >= pointLat && lowerRightLat <= pointLat)
      {
        true
      }
      else
      {
        false
      }
    }
    else
    {
      false
    }
  }

  val ST_Within = (pointString1: String, pointString2: String, distance: Double) =>
  {
    // Split the string into an array
    val p1 = pointString1.split(",")
    val p2 = pointString2.split(",")

    // Convert string array to double array
    val p1lat = p1(0).toDouble
    val p1Lon = p1(1).toDouble
    val p2Lat = p2(0).toDouble
    val p2Lon = p2(1).toDouble

    // Calculate euclidean distance between the two points
    val ourDistance = sqrt(pow(p1lat - p2Lat, 2) + pow(p1Lon - p2Lon, 2))

    // Check if the points are within the supplied distance
    if (ourDistance <= distance)
    {
      true
    }
    else
    {
      false
    }
  }



}
