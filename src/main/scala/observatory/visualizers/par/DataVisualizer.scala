package observatory.visualizers.par

import com.sksamuel.scrimage.Pixel
import observatory._
import observatory.visualizers.common.VisualizerConfiguration
import observatory.visualizers.common.interpolators.LinearInterpolator

import scala.collection.parallel.ParIterable

object DataVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)],
            configuration: VisualizerConfiguration): DataVisualizer =
    new DataVisualizer(temperatures, colors, configuration)
}

class DataVisualizer(val temperatures: Iterable[(Location, Temperature)],
                     val colors: Iterable[(Temperature, Color)],
                     val configuration: VisualizerConfiguration) extends ConfigurableVisuzlizer {
  private val interpolator = LinearInterpolator(colors)
  private val predictor = InverseDistancePredictor(configuration.epsilon, configuration.p)

  protected def computePixels(locations: ParIterable[Location]): Array[Pixel] = {
    val tempsInterpolated = predictor.predictTemperatures(temperatures, locations)
    val pixels = new Array[Pixel](locations.size)
    for {
      (temp, i) <- tempsInterpolated.zipWithIndex
    } {
      val color = interpolator.interpolateColor(temp)
      pixels(i) = Pixel(color.red, color.green, color.blue, configuration.transparency)
    }
    pixels
  }
}
