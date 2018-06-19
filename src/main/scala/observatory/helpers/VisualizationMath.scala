package observatory.helpers

import observatory.{Color, Location, Temperature}

import scala.collection.GenIterable
import scala.math._

object VisualizationMath {
  object Implicits {
    implicit def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
      math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

    implicit def tempDistance(a: Temperature, b: Temperature): Distance = abs(a - b)
  }

  import Implicits._
  private val R = 6372.8
  val COLOR_MAX = 255
  type Distance = Double

  def sphereDistance(a: Location, b: Location): Distance = {
    val dLat = (b.lat - a.lat).toRadians
    val dLon = (b.lon - a.lon).toRadians

    val exp = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(a.lat.toRadians) * cos(b.lat.toRadians)
    val distance = 2 * asin(sqrt(exp))
    R * distance
  }

  def w(x: Location, d: Distance, p: Double): Temperature = 1 / math.pow(d, p)

  def findTwoClosest(points: GenIterable[(Temperature, Color)], temp: Temperature)
                    (implicit tempDistance: (Temperature, Temperature) => Distance):
  ((Temperature, Color), (Temperature, Color)) = {
    case class PointDistance(point: (Temperature, Color), distance: Distance)
    val zero: (Option[PointDistance], Option[PointDistance]) = (None, None)
    val twoClosest = points.foldLeft(zero) {
      (agg: (Option[PointDistance], Option[PointDistance]), point: (Temperature, Color)) => {
        val newDistance = tempDistance(point._1, temp)
        agg match {
          case (Some(x), Some(y)) =>
            if (newDistance < x.distance)
              (Some(PointDistance(point, newDistance)), Some(y))
            else if (newDistance < y.distance)
              (Some(x), Some(PointDistance(point, newDistance)))
            else
              agg
          case (Some(x), None) =>
            (Some(x), Some(PointDistance(point, newDistance)))
          case _ =>
            (Some(PointDistance(point, newDistance)), None)
        }
      }
    }
    (twoClosest._1.get.point, twoClosest._2.get.point)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: GenIterable[(Temperature, Color)], value: Temperature)
                      (implicit interpolateComponent: (Temperature, Temperature, Temperature) => (Int, Int) => Int): Color = {
    def boundValue(min: Int, max: Int)(value: Int): Int = math.min(math.max(min, value), max)

    def bounder = boundValue(0, COLOR_MAX)(_)

    val ((x1: Temperature, y1: Color), (x2: Temperature, y2: Color)) = findTwoClosest(points, value)

    def interpolator = interpolateComponent(x1, x2, value)(_, _)

    val newRed = bounder(interpolator(y1.red, y2.red))
    val newGreen = bounder(interpolator(y1.green, y2.green))
    val newBlue = bounder(interpolator(y1.blue, y2.blue))
    Color(newRed, newGreen, newBlue)
  }
}
