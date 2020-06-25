package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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
}
