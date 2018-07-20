package observatory.visualizers.common.interpolators

import observatory.{Color, Temperature}

object LinearInterpolator {
  def apply(colors: Iterable[(Temperature, Color)]): LinearInterpolator = new LinearInterpolator(colors)
}
/**
  * Interpolates a temperature accordingly to given colors.
  * @param colors Pairs containing a value and its associated color
  */
class LinearInterpolator(colors: Iterable[(Temperature, Color)]) extends ColorsInterpolator with Serializable {
  // This class can be decomposed into trait with component interpolator and interval finder using Strategy pattern
  private val points = sortPoints(colors.toSeq)

  private def sortPoints(colors: Seq[(Temperature, Color)]): Seq[(Temperature, Color)] = {
    colors.sortBy { case (temp, _) => temp }
  }

  /**
    * Linear interpolation of a value between points x1, x2 with values y1, y2
    * @param x1 - left point
    * @param x2 - right value
    * @param value x coordinate of unknown value
    * @param y1 - left value
    * @param y2 - right value
    * @return y coordinate a searched value
    */
  private def interpolateComponent(x1: Temperature, x2: Temperature, value: Double)(y1: Int, y2: Int): Int =
    math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

  /**
    * For given sequence of points returns an interval which includes given temp point.
    * @param points sorted sequence of points
    * @param temp given point
    * @return Tuple(a, b) where a - is left border and b is right border. None corresponds to a infinity.
    */
  private def findInterval(points: Seq[(Temperature, Color)], temp: Temperature): (Option[(Temperature, Color)],
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
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(value: Temperature): Color = {
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
