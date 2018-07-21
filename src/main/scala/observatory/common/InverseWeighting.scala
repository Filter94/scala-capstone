package observatory.common

import observatory.{Location, Temperature}

import scala.math._

object InverseWeighting {
  // earth radius
  private val R = 6372.8
  type Distance = Double

  /**
    * https://en.wikipedia.org/wiki/Great-circle_distance
    * Computes great circle distance between location a and b
    */
  def sphereDistance(a: Location, b: Location): Distance = {
    val dLat = (b.lat - a.lat).toRadians
    val dLon = (b.lon - a.lon).toRadians

    val exp = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(a.lat.toRadians) * cos(b.lat.toRadians)
    val distance = 2 * asin(sqrt(exp))
    R * distance
  }

  /**
    * w parameter of a inverse distance weighting
    * @param d - distance between the point
    * @param p - sharpness power parameter
    * @return
    */
  def w(d: Distance, p: Double): Temperature = 1 / math.pow(d, p)
}
