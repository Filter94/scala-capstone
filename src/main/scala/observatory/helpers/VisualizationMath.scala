package observatory.helpers

import observatory.{Color, Location, Temperature}
import scala.math._

object VisualizationMath {
  private val R = 6372.8
  type Distance = Double

  def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
    math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt



  def sphereDistance(a: Location, b: Location): Distance = {
    val dLat = (b.lat - a.lat).toRadians
    val dLon = (b.lon - a.lon).toRadians

    val exp = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(a.lat.toRadians) * cos(b.lat.toRadians)
    val distance = 2 * asin(sqrt(exp))
    R * distance
  }

  def w(d: Distance, p: Double): Temperature = 1 / math.pow(d, p)

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
