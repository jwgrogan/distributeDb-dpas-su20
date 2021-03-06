package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.commons.collections.functors.TruePredicate

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def checkInBounds (bounds: List[Double], pointX: Int, pointY: Int, pointZ: Int): Boolean =
  {
    val minX = bounds(0)
    val maxX = bounds(1)
    val minY = bounds(2)
    val maxY = bounds(3)
    val minZ = bounds(4)
    val maxZ = bounds(5)

    if (pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY && pointZ >= minZ && pointZ <= maxZ)
    {
      true
    }
    else
    {
      false
    }
  }

  // takes point in table and returns list of nearest neighbors, accounting bounds of rectangle
  def getNeighborCount (bounds: List[Double], p: (Int, Int, Int)): Int =
  {
    var neighborCount = 0
    // iterate each possible point combination
    // each combination has the following options for each dimension: decrement, same, increment
    // iterate point list and apply modifications to each dimension
    var x = p._1
    var y = p._2
    var z = p._3
    for (i <- 0 to 2)
    {
      i match
      {
        case 0 => x = p._1 - 1
        case 1 => x = p._1
        case 2 => x = p._1 + 1
      }
      for (j <- 0 to 2)
      {
        j match
        {
          case 0 => y = p._2 - 1
          case 1 => y = p._2
          case 2 => y = p._2 + 1
        }
        for (k <- 0 to 2)
        {
          k match
          {
            case 0 => z = p._3 - 1
            case 1 => z = p._3
            case 2 => z = p._3 + 1
          }
          // add point to neighbors list if inbounds
          if (checkInBounds(bounds, x, y, z))
          {
            neighborCount += 1
          }
        }
      }
    }
    neighborCount
  }

  // Determine if two cells are neighbors
  def areNeighbors (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Boolean = {

    // To be a neighbor, a cell2's coordinate must be within +/- 1 along each axis relative to cell1
    // Eliminate cells that aren't neighbors by checking if they're outside each axis' limits
    if (x2 > x1 + 1 || x2 < x1 - 1)
      return false
    if (y2 > y1 + 1 || y2 < y1 - 1)
      return false
    if (z2 > z1 + 1 || z2 < z1 - 1)
      return false

    // If all coordinates are within the bounds, then cell2 is a neighbor of cell1
    true
  }

  def calculateGi (xi_sum : Int, wi_sum: Int, X_bar: Double, S: Double, n: Double): Double = {
    (xi_sum - X_bar * wi_sum) / (S * math.sqrt((n * wi_sum - math.pow(wi_sum, 2.0)) / (n - 1)))
  }
}
