package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

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

  private def ST_Contains(pointString: String, queryRectangle: String): Boolean =
  {
    // get point longitude and latitude
    val point = pointString.split(",")
    val pointLon = point(0).toDouble
    val pointLat = point(1).toDouble

    // get rectangle points
    val rectPoints = queryRectangle.split(",")
    val upperLeft = rectPoints.slice(0, 2)
    val upperLeftLon = upperLeft(0).toDouble
    val upperLeftLat = upperLeft(1).toDouble

    val lowerRight = rectPoints.slice(2, 4)
    val lowerRightLon = lowerRight(0).toDouble
    val lowerRightLat = lowerRight(1).toDouble

    // check longitude
    if (upperLeftLon <= pointLon && lowerRightLon >= pointLon)
    {
      // check latitude
      if (upperLeftLat >= pointLat && lowerRightLat <= pointLat)
      {
        return true
      }
    }
    return false
  }

  private def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean =
  {

  }

}
