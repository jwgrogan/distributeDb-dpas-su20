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

  def checkInBounds (bounds: List[Float], pointX: Int, pointY: Int, pointZ: Int): Boolean =
  {
    var minX = bounds(0)
    var maxX = bounds(1)
    var minY = bounds(2)
    var maxY = bounds(3)
    var minZ = bounds(4)
    var maxZ = bounds(5)

    if (pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY && pointZ >= minZ && pointZ <= maxZ)
    {
      return true
    }
    else
    {
      return false
    }
  }

  // takes point in table and returns list of nearest neighbors, accounting bounds of rectangle
  def getNeighbors (bounds: List[Float], pointX: Int, pointY: Int, pointZ: Int, count: Int): List[(Int, Int, Int)] =
  {
    var neighbors = List.newBuilder[(Int, Int, Int)]
    // iterate each possible point combination
    // each combination has the following options for each dimension: decrement, same, increment
    // iterate point list and apply modifications to each dimension
    var x = pointX
    var y = pointY
    var z = pointZ
    for (i <- 0 to 2)
    {
      i match
      {
        case 0 => x -= 1
        case 1 => x += 1
        case 2 => x += 1
      }
      for (j <- 0 to 2)
      {
        j match
        {
          case 0 => y -= 1
          case 1 => y += 1
          case 2 => y += 1
        }
        for (k <- 0 to 2)
        {
          k match
          {
            case 0 => z -= 1
            case 1 => z += 1
            case 2 => z += 1
          }
          // add point to neighbors list if inbounds
          if (checkInBounds(bounds, x, y, z))
          {
            neighbors += ((x, y, z))
          }
        }
      }
    }
    return neighbors.result()
  }
}
