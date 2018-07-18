package observatory.helpers.visualizers.par

import com.sksamuel.scrimage.Pixel
import observatory._
import observatory.helpers.{VisualizationHelper, VisualizerConfiguration}
import observatory.helpers.predictors.ParPredictor

import scala.collection.parallel.ParIterable

object Visualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)],
            configuration: VisualizerConfiguration): Visualizer =
    new Visualizer(temperatures, colors, configuration)
}

class Visualizer(val temperatures: Iterable[(Location, Temperature)],
                 val colors: Iterable[(Temperature, Color)],
                 val configuration: VisualizerConfiguration) extends ConfigurableVisuzlizer {
  private val colorsSorted = VisualizationHelper.sortPoints(colors.toSeq)
  private val predictor = ParPredictor(configuration.epsilon, configuration.p)

  protected def computePixels(locations: ParIterable[Location]): Array[Pixel] = {
    val tempsInterpolated = predictor.predictTemperatures(temperatures, locations)
    val pixels = new Array[Pixel](locations.size)
    for {
      (temp, i) <- tempsInterpolated.zipWithIndex
    } {
      val color = VisualizationHelper.interpolateColor(colorsSorted, temp)
      pixels(i) = Pixel(color.red, color.green, color.blue, configuration.transparency)
    }
    pixels
  }
}
