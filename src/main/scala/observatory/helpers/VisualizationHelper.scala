package observatory.helpers

import observatory.{Color, Location, Temperature}
import scala.math._

object VisualizationHelper {
  // earth radius
  private val R = 6372.8
  type Distance = Double

  /**
    * Linear interpolation of a value between points x1, x2 with values y1, y2
    * @param x1 - left point
    * @param x2 - right value
    * @param value x coordinate of unknown value
    * @param y1 - left value
    * @param y2 - right value
    * @return y coordinate a searched value
    */
  def interpolateComponent(x1: Temperature, x2: Temperature, value: Double)(y1: Int, y2: Int): Int =
    math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

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

  /**
    * For given sequence of points returns an interval which includes given temp point.
    * @param points sorted sequence of points
    * @param temp given point
    * @return Tuple(a, b) where a - is left border and b is right border. None corresponds to a infinity.
    */
  def findInterval(points: Seq[(Temperature, Color)], temp: Temperature): (Option[(Temperature, Color)],
    Option[(Temperature, Color)]) = {
    val rightWithIndex = points.zipWithIndex.find {
      case ((colorTemp: Temperature, _: Color), _) => temp <= colorTemp
    }
    rightWithIndex match {
      case Some((left, i)) =>
        if (i > 0) {
          val right = points(i - 1)
          (Some(right), Some(left))
        } else {
          (Some(left), None)
        }
      case None =>
        (None, Some(points.last))
    }
  }

  def sortPoints(colors: Seq[(Temperature, Color)]): Seq[(Temperature, Color)] = {
    colors.sortBy { case (temp, _) => temp }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Seq[(Temperature, Color)], value: Temperature): Color = {
    findInterval(points, value) match {
      case (Some(left), Some(right)) =>
        val (x1, y1) = left
        val (x2, y2) = right

        def interpolator = interpolateComponent(x1, x2, value)(_, _)

        Color(interpolator(y1.red, y2.red),
          interpolator(y1.green, y2.green),
          interpolator(y1.blue, y2.blue))
      case (Some(left), None) =>
        left._2
      case (None, Some(right)) =>
        right._2
    }
  }
}
