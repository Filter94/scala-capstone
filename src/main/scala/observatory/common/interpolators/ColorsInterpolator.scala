package observatory.common.interpolators

import observatory.{Color, Temperature}

trait ColorsInterpolator {
  def interpolateColor(temp: Temperature): Color
}
