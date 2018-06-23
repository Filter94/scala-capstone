package observatory.helpers

import observatory.{Color, Location, Temperature}
import scala.math._

object VisualizationMath {
  def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
    math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

  def tempDistance(a: Temperature, b: Temperature): Distance = abs(a - b)

  val epsilon = 1E-5
  private val R = 6372.8
  val COLOR_MAX = 255
  type Distance = Double

  def sphereDistance(a: Location, b: Location): Distance = {
    if (a == b) {
      0
    } else {
      val lat1 = toRadians(a.lat)
      val lat2 = toRadians(b.lat)
      val deltaLongitudes = abs(toRadians(a.lon - b.lon))
      R * acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(deltaLongitudes))
    }
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
