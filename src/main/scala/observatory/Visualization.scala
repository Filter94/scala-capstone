package observatory

import com.sksamuel.scrimage.Image
import observatory.visualizers.common.VisualizerConfiguration
import observatory.visualizers.common.interpolators.LinearInterpolator
import observatory.visualizers.par.InverseDistancePredictor
import observatory.visualizers.spark.DataVisualizer

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
    InverseDistancePredictor(1e-5, 3).predictTemperature(temperatures, location)
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
    val config = new VisualizerConfiguration.Builder().build
    DataVisualizer(temperatures, colors, config).visualize()
  }

}

