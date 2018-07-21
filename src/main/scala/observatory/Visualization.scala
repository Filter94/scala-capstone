package observatory

import com.sksamuel.scrimage.Image
import observatory.common.InverseWeightingConfiguration
import observatory.common.interpolators.LinearInterpolator
import observatory.parimpl.ParInverseWeighingPredictor
import observatory.visualizers.par.DefaultVisualizer

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    ParInverseWeighingPredictor(temperatures, InverseWeightingConfiguration.Builder().build)
      .predictTemperature(location)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    LinearInterpolator(points).interpolateColor(value)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    DefaultVisualizer(temperatures, colors).visualize()
  }

}

